(ns rowboat.elastic
  (:require [clojure.string :as str]
            [jsonista.core :as json]
            [rowboat.tools :as tools]
            [clojure.tools.logging :as log])
  (:import (java.util.concurrent TimeUnit)
           (scala.collection.immutable Map Map$ Nil$)
           (scala.concurrent.duration FiniteDuration)
           (akka.stream.alpakka.elasticsearch.scaladsl ElasticsearchSource)
           (akka.stream.alpakka.elasticsearch ElasticsearchParams ElasticsearchSourceSettings ElasticsearchConnectionSettings)))

(defn elastic-settings [{:keys [url auth settings]}]
  (let [username (first auth)
        password (second auth)
        {:keys [buffer-size scroll-duration]} settings]
    (-> (ElasticsearchConnectionSettings/apply url)
        (.withCredentials username password)
        ElasticsearchSourceSettings/apply
        ^ElasticsearchSourceSettings (.withBufferSize buffer-size)
        (.withScrollDuration (FiniteDuration/apply ^long scroll-duration TimeUnit/MINUTES)))))

(defn request->scala [{:keys [fields query sort]}]
  (-> (.empty Map$/MODULE$)
      ^Map (.updated "sort" (json/write-value-as-string sort))
      ^Map (.updated "query" (json/write-value-as-string query))
      ^Map (.updated "_source" (json/write-value-as-string fields))))

(defn collect-nested [record [head & rest]]
  "Elastic response can contain arrays of child entities,
   whose attributes are to be aggregated for the parent.
   For example, a person can have addresses, which have
   zips (which may have duplicates):

   {:first-name \"Joe\"
    :address [{:zip \"18966\"}
              {:zip \"10001\"}
              {:zip \"18966\"}]}

   The Elastic attribute for the zip values is,
   in this example, `address.zip`. But it's not a key.
   It can be split into a path, though: `[:address :zip]`.
   Thus, we want a function that collects all values
   from a json-derived entity for such a path,
   while also eliminating duplicates.

  (collect-nested {:a [{:b {:c [{:d 1}
                                {:d 2}]}}
                       {:b {:c [{:d 1}
                                {:d 3}]}}]}
                  [:a :b :c :d]))
  => [1 2 3]"
  (let [value (record head)]
    (cond
      (map? value)  (collect-nested value rest)
      (coll? value) (->> value
                         (map #(collect-nested % rest))
                         flatten
                         (remove nil?)
                         distinct
                         vec)
      (seq rest)    nil ;; the path is longer than the entity's tree.
      :else         value)))

(defn row-transformer [fields]
  (fn [record]
    (reduce (fn [acc field]
              (let [path (->> (str/split field #"\.")
                              (mapv keyword))
                    value (collect-nested record path)]
                (.$colon$colon acc value)))
            Nil$/MODULE$
            (reverse fields))))

(defn create-elastic-source [{:keys [settings index query fields]}]
  (log/debug "Elastic query:" query)
  (let [transform-fn (row-transformer fields)]
    (-> (ElasticsearchSource/create
          (ElasticsearchParams/V7 index)
          ^Map (request->scala query)
          ^ElasticsearchSourceSettings settings)
        (.map (tools/fn1->scala
                #(-> % ;; ReadResult[JsObject] in Scala
                     .source
                     .toString
                     json/write-value-as-string
                     transform-fn))))))
