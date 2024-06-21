(ns rowboat.jdbc
  (:require [next.jdbc :as jdbc]
            [rowboat.tools :as tools]
            [rowboat.streams :as streams]
            [clojure.tools.logging :as log]
            [camel-snake-kebab.core :as csk]
            [rowboat.constants :as constants])
  (:import (scala.collection.immutable Nil$)))

(defn row-transformer [fields]
  (let [columns (map csk/->snake_case_keyword fields)]
    (fn [record]
      (let [item (tools/strip-namespaces-from-keys record)]
        (tools/seq->scala columns #(get item %))))))

(defn create-db-source [{:keys [datasource opts query fields]}]
  (log/debug "SQL query:" query)
  (let [fetch-size (or (:fetch-size opts) constants/default-chunk-size)
        ;; It is important to set `auto-commit` to false.
        ;; Otherwise, `next.jdbc/plan` can't count on transactionality when fetching
        ;; the results chunk by chunk. Therefore, if `auto-commit` is set to true,
        ;; it will not use cursors but instead will load everything at once.
        reducible  (jdbc/plan datasource [query] (-> opts
                                                     (assoc :read-only true)
                                                     (assoc :cursors :close)
                                                     (assoc :auto-commit false)
                                                     (assoc :fetch-size fetch-size)
                                                     (assoc :concurrency :read-only)
                                                     (assoc :result-type :forward-only)))
        iterator   (streams/reducible->iterator reducible {:chunk-size    fetch-size
                                                           :empty-element Nil$/MODULE$
                                                           :transform-fn  (row-transformer fields)
                                                           :executor      tools/default-blocking-executor})]
    (streams/iterator->source iterator)))
