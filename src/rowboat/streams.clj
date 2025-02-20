(ns rowboat.streams
  (:require [rowboat.tools :as tools]
            [clojure.tools.logging :as log]
            [rowboat.constants :as constants]
            [clojure.core.async :refer [>!! <!!]])
  (:import (scala Function0)
           (scala.concurrent Future)
           (akka.stream Inlet Outlet)
           (scala.collection Iterator)
           (scala.collection.immutable Nil$)
           (akka.stream.scaladsl Source Sink)
           (akka.stream SourceShape SinkShape)
           (java.util.concurrent ArrayBlockingQueue)
           (akka.stream.scaladsl GraphDSL$Builder GraphDSL$Implicits$)
           (akka.stream.stage GraphStage GraphStageLogic InHandler OutHandler)))

(defn reducible->iterator
  "The libraries like next.jdbc favor Clojure reducibles (IReduceInit),
  which can only be processed by reducing them with a transducer function.
  Unfortunately, `reduce` consumes the reducible's contents eagerly.
  For streaming purposes, to handle backpressure, we want a lazy iterator,
  whose items would be consumed only when processing capacity is
  available on the consuming end.

  The above conundrum is resolved as follows. A blocking queue of a fixed
  size is created, and the transducer function attempts to fill it with
  the reducible data. However, when the queue is full, the transducer
  is blocked, waiting upon the consumer to take some items off the queue.
  This taking off occurs in the reified iterator's `next` method.
  Thus, the processing occurs lazily, in chunks of fixed size.

  If the reducible holds a resource that's released only upon
  a successful completion of `reduce` (next.jdbc automatically
  closes a database connection only when reducing is complete),
  then a leak may be introduced if the iterator isn't run to the end.
  Therefore, a `drain` function is also introduced, to be called on
  the iterator returned by `reducible->iterator` in case of error.

  The atom flag is necessary to address the race condition that arises
  when the consumer is faster than the producer, so that it doesn't stop
  prematurely, if the queue is empty but the producer isn't yet done.
  Although volatiles are generally recommended to hold state in stateful
  transducers, since they are more performant (and less capable) than
  atoms, in this case, an atom was found to be required, as the Akka
  streams didn't appear to respect a Clojure volatile.

  Since `(.take queue)` will block if the queue is empty, we must have
  the `next` method return a Scala Future (otherwise, the Akka streams
  may, not always, time out). The Akka Streams documentation recommends that
  an iterator with blocking actions be run in a separate execution context.
  Hence, the `execution-context` above. When this iterator is transformed
  to an Akka Source, that source must be converted from Source[Future[T]]
  to Source[T] by calling `mapAsync`."
  [reducible
   {:keys [preload-items postload-items transform-fn
           chunk-size empty-element executor]
    :or {transform-fn  identity
         empty-element Nil$/MODULE$
         chunk-size    constants/default-chunk-size
         executor      tools/default-blocking-executor}}]
  (let [context (tools/executor->execution-context executor)
        queue   (ArrayBlockingQueue. chunk-size)
        state   (atom {:done?    false
                       :pressure 0
                       :in-count 0
                       :out-count 0})
        push    (fn [item]
                  (swap! state update :pressure inc)
                  (.put queue (transform-fn item)))
        pop     (fn []
                  (swap! state update :pressure dec)
                  (.take queue))]
    (future
      (try
        (mapv #(.put queue %) preload-items)
        (reduce (fn [_ item] (push item))
                nil reducible)
        (mapv #(.put queue %) postload-items)
        (catch Throwable error
          (log/error "Error while enqueuing a JDBC reducible:" error)))
      (swap! state assoc :done? true)
      ;; In case more `next` calls have been made in between the last `put` and `swap!!`
      ;; than there were elements in the queue. Just in case, unblock the extra calls
      ;; with empty elements.
      (let [{:keys [pressure]} @state]
        (when (> 0 pressure)
          (log/debug "Detected negative pressure after enqueuing a JDBC reducible:" pressure)
          (repeatedly (- pressure) (.put queue empty-element)))))
    (reify Iterator
      (hasNext [_]
        (not (and (:done? @state)
                  (.isEmpty queue))))
      (next [_]
        (if (.isEmpty queue)
          ;; If the queue is empty, (.take queue) will block.
          ;; This won't make Akka Streams happy.
          (Future/apply
            (reify Function0 ;; That's how Scala's call-by-name looks like to JVM.
              (apply [_] (pop)))
            context)
          ;; Spare a thread for waiting on, if the queue isn't empty.
          (Future/successful (pop)))))))

(defn iterator->source [iterator]
  (-> (Source/fromIterator
        (reify Function0
          (apply [_]
            iterator)))
      ;; In Scala, if x is a Source[Future[T]], then
      ;; x.mapAsync(parallelism)(identity) is a Source[T].
      (.mapAsync 1 tools/scala-identity)))

(defn pipe
  "Connect an outlet to an inlet in an Akka graph.
   The builder is passed as an argument to each GraphDSL building block."
  [outlet inlet builder]
  ;; In Scala, this entire function is a single line: `outlet ~> inlet`.
  ;; In JVM, the `~>` method is represented as `$tilde$greater`, so this is how we must call it.
  ;; But that method lives in an interface that isn't implemented by `Outlet`.
  ;; So, how can it be invoked on `outlet` here, which is an instance of `Outlet`?
  ;; This is because of Scala's implicit conversions. In Scala, it's enough to include
  ;; `import GraphDSL.Implicits._` to bring all methods marked with the `implicit` keyword
  ;; into the scope. If an implicit method is found that takes an `Outlet` as an argument
  ;; and returns something that implements `$tilde$greater`, then `$tilde$greater` can be
  ;; applied to an instance of `Outlet` by calling the conversion function first.
  ;; In Clojure, however, we have to be explicit about it. So, here's how to:
  ;; `GraphDSL.Implicits` is a nested object, represented in JVM as `GraphDSL$Implicits$/MODULE$`
  ;; and `port2flow` is its implicit method that converts `Outlet` to `CombinerBase`.
  (-> GraphDSL$Implicits$/MODULE$
      (.port2flow outlet builder)
      (.$tilde$greater ^Inlet inlet
                       ^GraphDSL$Builder builder)))

(defn channel->source
  "Creates an Akka source from a Clojure channel.
   The source emits the elements that are retrieved from the channel."
  [channel name]
  (let [outlet  (Outlet/apply name)               ;; Each source has no inlets and a single outlet.
        shape   (SourceShape/apply outlet)        ;; The shape of the source: zero inlets, one outlet.
        logic   (proxy [GraphStageLogic] [shape]) ;; Instantiate this abstract class with the constructor that has Shape as argument.
        handler (reify OutHandler                 ;; The handler must override `onPull` (when data is requested from the downstream).
                  (onPull [_]
                    ;; When the next element is requested, read from the channel
                    ;; and then push the retrieved data through the outlet.
                    (->> channel
                         <!!
                         (.push logic outlet))))
        graph   (proxy [GraphStage] [] ;; Instantiate this abstract class with its default constructor.
                  (shape [] shape)
                  (createLogic [_]
                    ;; It's important to attach the handler ONLY when `createLogic` is called.
                    ;; Otherwise, the graph interpreter may not be ready yet.
                    (.setHandler logic outlet handler)
                    logic))]
    (Source/fromGraph graph)))

(defn channel->sink
  "Creates an Akka sink from a Clojure channel.
   All elements passed to the sink are sent to the channel."
  [channel name]
  (let [inlet   (Inlet/apply name)               ;; Each sink has a single inlet and no outlets.
        shape   (SinkShape/apply inlet)          ;; The shape of the sink: one inlet, zero outlets.
        logic   (proxy [GraphStageLogic] [shape] ;; Instantiate this abstract class with the constructor that has Shape as argument.
                  ;; Override the `preStart` method to initiate the flow
                  ;; by requesting the first element from the upstream.
                  ;; On the reasons for doing this and other info on custom Akka flows,
                  ;; please consult https://doc.akka.io/docs/akka/current/stream/stream-customize.html
                  (preStart [] (.pull this inlet)))
        handler (reify InHandler ;; The handler must override `onPush` (when data is received from the upstream).
                  (onPush [_]
                    ;; Grab the data from the inlet and send it to the channel.
                    (->> inlet
                         (.grab logic)
                         (>!! channel))
                    (.pull logic inlet)))
        graph   (proxy [GraphStage] [] ;; Instantiate this abstract class with its default constructor.
                  (shape [] shape)
                  (createLogic [_]
                    ;; It's important to attach the handler ONLY when `createLogic` is called.
                    ;; Otherwise, the graph interpreter may not be ready yet.
                    (.setHandler logic inlet handler)
                    logic))]
    (Sink/fromGraph graph)))
