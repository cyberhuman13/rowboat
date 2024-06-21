(ns rowboat.core
  (:require [yang.lang :as lang]
            [rowboat.aws :as aws]
            [rowboat.jdbc :as jdbc]
            [rowboat.tools :as tools]
            [rowboat.streams :as streams]
            [rowboat.elastic :as elastic]
            [clojure.tools.logging :as log]
            [rowboat.constants :as constants]
            [clojure.core.async :refer [chan]])
  (:import (scala Tuple2 None$)
           (akka.actor ActorSystem)
           (scala.util Failure Try)
           (java.nio.charset StandardCharsets)
           (scala.collection.immutable Range Range$)
           (akka.stream.alpakka.file ArchiveMetadata)
           (akka.stream.alpakka.file.scaladsl Archive)
           (akka.stream.alpakka.csv.scaladsl CsvFormatting CsvQuotingStyle$Required$)
           (akka.stream.scaladsl GraphDSL GraphDSL$Builder RunnableGraph Partition Source Sink Unzip)
           (akka.stream Materializer SourceShape SinkShape FlowShape FanOutShape2 UniformFanOutShape ClosedShape$)))

(defonce system (ActorSystem/create "extract"))

(def csv-formatting
  (CsvFormatting/format
    ;; the default arguments follow
    (CsvFormatting/Comma)
    (CsvFormatting/DoubleQuote)
    (CsvFormatting/Backslash)
    "\r\n"
    CsvQuotingStyle$Required$/MODULE$
    StandardCharsets/UTF_8
    None$/MODULE$))

(defn run-extract-single
  "A much simpler Akka graph is used for most extract requests,
   when only file CSV file is to be produced."
  [source sink headers {:keys [csv-filename size]}]
  (let [csv   (-> source                             ;; The original source (from SQL db or Elastic).
                  (.take size)                       ;; Trim the source to size.
                  (.prepend (Source/single headers)) ;; Prepend the header row.
                  (.via csv-formatting))             ;; Format the records as CSV.
        graph (-> (Tuple2/apply ;; Attach the filename to the CSV stream.
                    (ArchiveMetadata/create (str csv-filename ".csv"))
                    csv)
                  Source/single ;; We need a source of tuples as above (only a single one, in this case).
                  (.via (Archive/zip)))] ;; Send the above source via the zipping flow.
    ;; Run the graph to the provided S3 sink.
    ;; Above, we constructed the graph. But only now,
    ;; when `.runWith` is executed, the data begins to flow.
    (.runWith graph sink (Materializer/matFromSystem system))))

(defn run-extract-multiple
  "When we must split the data into multiple CSV files (based on `max-csv-file-row-count`),
   a more complex Akka graph must be used. Instead of a single straight line, it must
   now fork the original source into several streams, one per CSV file."
  [source sink headers {:keys [csv-filename size max-csv-file-row-count wormhole-buffer-size]}]
  (let [chunk-fn       #(-> % dec (quot max-csv-file-row-count) int) ;; For a given record's index, determine its partition.
        chunk-count    (-> size chunk-fn inc)                        ;; The partition count.
        ;; Create a Scala range (0 to chunk-count) exclusive, since it's more efficient to convert
        ;; Clojure functions to Scala functions than Clojure collections to Scala ones.
        range          (.apply Range$/MODULE$ (int 0) chunk-count)
        ;; For each partition (CSV file), create a Clojure channel.
        ;; These will serve as a "wormhole" to connect two Akka graphs.
        ;; If the buffer fills up, it blocks the writer, which backpressures everything upstream.
        wormhole       (.map range (tools/fn1->scala ;; Converts a Clojure function to Scala.
                                     (fn [_] (chan wormhole-buffer-size))))
        materializer   (Materializer/matFromSystem system)
        ;; Create a source of (record, index) tuples,
        ;; because partitioning is based on indexing.
        indexed-source (.zipWithIndex source)
        build-block    (fn [^GraphDSL$Builder builder] ;; The GraphDSL building block.
                         ;; The builder is mutated whenever a new node is added to the graph via `.add`.
                         ;; This creates the graph's traversal stages inside the builder.
                         ;; But all of that is hidden and not really necessary to know by the caller.
                         ;; Only mentioning this here to indicate the importance of `.add` calls.
                         (let [^SourceShape input         (.add builder indexed-source) ;; Add the source to the graph.
                               ;; Create a partition, fanning the source out into portions, one per CSV file.
                               ;; The partition is of the UniformFanOutShape, which has one inlet and many outlets.
                               ^UniformFanOutShape fanout (->> #(-> % ._2 chunk-fn)
                                                               tools/fn1->scala
                                                               (Partition/apply chunk-count)
                                                               (.add builder))] ;; Add the partition to the graph.
                           ;; For each chunk of the partition, perform the necessary plumbing.
                           ;; Each outlet of the partition emits the records going into one CSV file.
                           ;; So, the record from that outlet must to the inlet of a CSV-formatting flow.
                           ;; But recall that the data coming out of the outlet is in tuples: (record, index).
                           ;; First, we must strip the index. This can be accomplished by passing the tuples
                           ;; though another fan-out, which splits the stream of (record, index) into
                           ;; a stream of records and a stream of indexes, then ignore the latter.
                           ;; For that, we must create a sink to ignore them, since the graph
                           ;; must be completely closed. Another sink will accept the records.
                           ;; That sink will be constructed from the "wormhole" Clojure channels created earlier.
                           ;;
                           ;;                                 source [records]
                           ;;                                        |
                           ;;                         indexed source [(record, index)]
                           ;;                                        |
                           ;;                                partition by index
                           ;;              __________________________|__________________________
                           ;;              |                         |                         |
                           ;;      [(record, index)]         [(record, index)]         [(record, index)]
                           ;;              |                         |                         |
                           ;;           stripper                  stripper                  stripper
                           ;;         _____|______              _____|______              _____|______
                           ;;         |          |              |          |              |          |
                           ;;     [records]  [indexes]      [records]  [indexes]      [records]  [indexes]
                           ;;        |           |              |          |              |          |
                           ;;     formatter    ignore       formatter    ignore       formatter    ignore
                           ;;        |                          |                         |
                           ;;     wormhole                  wormhole                  wormhole
                           ;;        |                          |                         |
                           ;;    csv-source                csv-source                csv-source
                           ;;        |__________________________|_________________________|
                           ;;                                   |
                           ;;                                 zipper
                           ;;                                   |
                           ;;                                s3-sink
                           (.foreach range (tools/fn1->scala
                                             (fn [port] ;; port is the chunk's number in the partition: 0 to chunk-size - 1.
                                               (let [channel                (.apply wormhole port)                ;; The channel corresponding to this chunk.
                                                     name                   (str "channel-sink-" port)            ;; The name of the channel-based sink.
                                                     channel-sink           (streams/channel->sink channel name)  ;; The sink to send the records to the wormhole channel.
                                                     ^FlowShape formatter   (.add builder csv-formatting)         ;; Add a CSV-formatting flow to the graph.
                                                     ^SinkShape ignore      (.add builder (Sink/ignore))          ;; Add a sink to ignore the indexes.
                                                     ^SinkShape sink        (.add builder channel-sink)           ;; Add the wormhole sink to the graph.
                                                     ^FanOutShape2 stripper (.add builder (Unzip.))]              ;; This one strips the index off.
                                                 ;; Connect the partition's outlet (with the number `port`) to the stripper's inlet.
                                                 (streams/pipe (.out fanout port) (.in stripper) builder)
                                                 ;; Connect the stripper's outlet that emits records to the formatter's inlet.
                                                 (streams/pipe (.out0 stripper) (.in formatter) builder)
                                                 ;; Connect the stripper's outlet that emits indexes to the ignored sink.
                                                 (streams/pipe (.out1 stripper) (.in ignore) builder)
                                                 ;; Connect the formatter's outlet to the wormhole sink's inlet.
                                                 (streams/pipe (.out formatter) (.in sink) builder)))))
                           ;; Finally, connect the source's outlet to the partition's inlet.
                           (streams/pipe (.out input) (.in fanout) builder)
                           ;; Now that no inlets and outlets remain unconnected, the graph is completely closed.
                           ;; So, return `ClosedShape`, the shape of the graph that will be built for this
                           ;; building block. This is why we needed the wormhole sinks. Without them, the
                           ;; graph would have had no inlets and N uncnnnected outlets (one per CSV file).
                           ;; The graph constructed then would have been of `AmorphousShape`, while what
                           ;; we need is a collection of sources to pass to `(Archive/zip)`.
                           ClosedShape$/MODULE$))
        csv-sources    (-> wormhole ;; Construct a collection of sources, one per wormhole channel.
                           ^Range .zipWithIndex
                           (.map (tools/fn1->scala
                                   (fn [tuple]
                                     (let [channel  (._1 tuple) ;; The wormhole channel.
                                           port     (._2 tuple) ;; The partition's index.
                                           name     (str "channel-source-" port)
                                           size     (if (= port (dec chunk-count)) ;; The size of the stream through this chunk.
                                                      (mod size max-csv-file-row-count)
                                                      max-csv-file-row-count)
                                           filename (str csv-filename ;; The filename must be modified: XXX_0.csv, XXX_1.csv, etc.
                                                         (when (> chunk-count 1) (str "_" port))
                                                         ".csv")
                                           top-row  (-> headers ;; Don't forget to CSV-format the headers, before prepending them.
                                                        Source/single
                                                        (.via csv-formatting))]
                                       (Tuple2/apply ;; Attach the filename to the wormhole source created for this chunk.
                                         (ArchiveMetadata/create filename)
                                         (-> channel                        ;; The wormhole channel.
                                             (streams/channel->source name) ;; The wormhole sink created for this channel.
                                             (.take size)                   ;; The size of the source tells us when the flow is complete.
                                             (.prepend top-row))))))))]     ;; Prepend the header row to the source.
    ;; Build the closed graph that sends CSV-formatted
    ;; data into the wormhole channels and run it.
    ;; We don't care what this Scala future returns.
    (-> build-block
        tools/fn1->scala
        GraphDSL/create
        RunnableGraph/fromGraph
        (.run materializer))
    ;; Create a source of sources that takes the CSV-formatted
    ;; data from the wormhole channels and zips it. Return this Scala future.
    ;; When this Scala future completes, then we're done.
    (-> csv-sources
        Source/apply
        (.via (Archive/zip)) ;; Pass the data through the zipping flow.
        (.runWith sink materializer))))

(defn run-extract [source headers
                   {:keys [bucket zip-filename s3-settings size
                           max-csv-file-row-count chunking-parallelism] :as params}]
  ;; Create the S3 Akka sink that the data will ultimately be sent into.
  (let [sink (aws/s3-sink s3-settings bucket zip-filename
                          {:content-type         :zip
                           :chunking-parallelism chunking-parallelism})]
    ;; If there is more than one CSV file to be zipped,
    ;; we will need a more complex Akka graph.
    ;; Otherwise, follow the simple path.
    (if (> size max-csv-file-row-count)
      (run-extract-multiple source sink headers params)
      (run-extract-single source sink headers params))))

(defn produce-extract [{:keys [id size fields aliases type query] :as event}
                       {:keys [aws-config bucket elastic index datasource opts
                               wormhole-buffer-size chunking-parallelism max-csv-file-row-count]
                        :or {wormhole-buffer-size   constants/default-wormhole-buffer-size
                             chunking-parallelism   constants/default-chunking-parallelism
                             max-csv-file-row-count constants/default-max-csv-file-row-count}
                        :as _config}
                       {:keys [on-start on-success on-failure on-empty] :as _callbacks}]
  {:pre [(and size (> size 0))]}
  (let [extract-type  (keyword type)
        extract-id    (or id (lang/squuid))
        event-with-id (assoc event :id extract-id)
        zip-filename  (tools/s3-filename event-with-id)
        csv-filename  (tools/csv-filename event-with-id)
        s3-settings   (aws/s3-settings system aws-config)]
    (try
      (if (and (> size 0) (seq fields))
        (do
          (on-start event-with-id)
          (let [headers (tools/fields->headers fields aliases)
                source  (case extract-type
                          :jdbc    (jdbc/create-db-source {:query      query
                                                           :fields     fields
                                                           :datasource datasource
                                                           :opts       opts})
                          :elastic (elastic/create-elastic-source {:query    query
                                                                   :fields   fields
                                                                   :settings elastic
                                                                   :index    index}))]
            ;; This was all preparation. Now comes the real thing: the run-extract call.
            (-> (run-extract source headers
                             {:size                   size
                              :bucket                 bucket
                              :s3-settings            s3-settings
                              :csv-filename           csv-filename
                              :zip-filename           zip-filename
                              :max-csv-file-row-count max-csv-file-row-count
                              :wormhole-buffer-size   wormhole-buffer-size
                              :chunking-parallelism   chunking-parallelism})
                (.onComplete (tools/fn1->scala
                               (fn [result]
                                 (let [failed? (.isFailure ^Try result)
                                       error   (when failed? (.exception ^Failure result))]
                                   (if failed?
                                     (do
                                       (log/error "Error when processing extract:" error)
                                       (on-failure (assoc event-with-id :error error)))
                                     (on-success (-> event-with-id
                                                     (assoc :size size)
                                                     (assoc :filename zip-filename)))))))
                             (.dispatcher system)))
            ;; Return the S3 URI to the zipped file.
            (str "s3://" bucket "/" zip-filename)))
        (on-empty event-with-id))
      ;; This catch clause is for catching errors when preparing the extract.
      ;; Those thrown during execution, inside a Scala future are processed above.
      ;; But if an error is thrown elsewhere, then it's caught here.
      (catch Throwable error
        (log/error "Error when preparing extract:" error)
        (on-failure (assoc event-with-id :error error))))))
