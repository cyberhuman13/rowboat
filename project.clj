;; this is until we get certs (HTTPS) for the build server
#_(cemerick.pomegranate.aether/register-wagon-factory! "http"
  #(org.apache.maven.wagon.providers.http.HttpWagon.))

(defproject silverberry13/rowboat "0.1.0"
  :dependencies [[tick/tick "0.7.5"]
                 [tolitius/yang "0.1.41"]
                 [metosin/jsonista "0.3.8"]
                 [org.clojure/clojure "1.11.3"]
                 [org.clojure/core.async "1.6.681"]
                 [org.clojure/tools.nrepl "0.2.13"]
                 [org.clojure/tools.logging "1.3.0"]
                 [ch.qos.logback/logback-core "1.5.6"]
                 [ch.qos.logback/logback-classic "1.5.6"]
                 [com.github.seancorfield/next.jdbc "1.3.939"]
                 [com.lightbend.akka/akka-stream-alpakka-s3_2.13 "6.0.2"]
                 [com.lightbend.akka/akka-stream-alpakka-csv_2.13 "6.0.2"]
                 [com.lightbend.akka/akka-stream-alpakka-file_2.13 "6.0.2"]
                 [com.lightbend.akka/akka-stream-alpakka-elasticsearch_2.13 "6.0.2"]]

  :source-paths   ["src"]
  :test-paths     ["test"]
  :resource-paths ["resources"]

  ;; Necessary to properly merge Akka configuration files.
  ;; Note the first line is the usual default.
  :uberjar-merge-with {#"\.properties$" [slurp str spit]
                       "reference.conf" [slurp str spit]}

  :profiles {:dev {:source-paths   ["dev"]
                   :resource-paths ["dev-resources"]
                   :dependencies   [[clj-http/clj-http "3.13.0"]         ;; To make HTTP calls against Elastic in REPL
                                    [hikari-cp/hikari-cp "3.0.1"]        ;; To create a JDBC datasource in REPL
                                    [org.postgresql/postgresql "42.3.6"] ;; To test against PostgreSQL in REPL
                                    [org.clojure/java.classpath "1.1.0"]
                                    [org.clojure/tools.namespace "1.5.0"]]
                   :repl-options   {:init-ns dev}
                   :jvm-opts       ["-Xmx2g"]}
             :jar {:resource-paths ["target/about"]}}

  :plugins [[lein-ancient "1.0.0-RC4-SNAPSHOT"]]

  :repositories [["clojars" {:url "https://repo.clojars.org/"}]
                 ["maven-central" {:url "https://repo1.maven.org/maven2"}]])
