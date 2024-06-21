(ns rowboat.tools
  (:require [tick.core :as tick]
            [yang.lang :as lang]
            [clojure.string :as str]
            [rowboat.constants :as constants])
  (:import (scala Function1)
           (java.util.concurrent Executors)
           (scala.collection.immutable Nil$)
           (scala.concurrent ExecutionContext)))

(defonce default-blocking-executor
  (Executors/newFixedThreadPool constants/default-thread-pool-size))

(defn executor->execution-context [executor]
  (ExecutionContext/fromExecutor executor))

(defn fn1->scala [f]
  (reify Function1
    (apply [_ arg]
      (f arg))))

(def scala-identity
  (fn1->scala identity))

(defn seq->scala
  ([coll]
   (seq->scala coll identity))
  ([coll transform-fn]
   (reduce (fn [acc elem]
             (.$colon$colon acc (transform-fn elem)))
           Nil$/MODULE$
           (reverse coll))))

(defn kebab->pascal-space [s]
  (when (lang/sval? s)
    (let [words (str/split s #"-")]
      (str/join " " (map str/capitalize words)))))

(defn fields->headers
  ([fields]
   (fields->headers fields nil))
  ([fields aliases]
   (seq->scala fields (fn [field]
                        (or ((keyword field) aliases)
                            (kebab->pascal-space (name field)))))))

(defonce formatter
  (tick/formatter "yyyy-MM-dd_HH-mm-ss"))

(defn csv-filename [{:keys [prefix] :or {prefix constants/default-prefix}}]
  (str prefix "_" (tick/format formatter (tick/date-time)) "_GMT"))

(defn s3-filename [{:keys [id prefix] :or {prefix constants/default-prefix}}]
  (str prefix "-" id ".zip"))

(defn strip-namespaces-from-keys
  "Remove namespaces from the record's keywords: :a/b => :b"
  [item]
  {:pre [(or (map? item) (nil? item))]}
  (when item
    (update-keys item (comp keyword name))))
