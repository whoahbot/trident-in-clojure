(defproject trident-in-clojure "0.1.0-SNAPSHOT"
  :description "An example Trident topology written in clojure."
  :license {:name "Apache Public License - v 1.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :source-paths ["src/main/clojure"]
  :test-paths ["src/test/clojure"]
  :dependencies [[storm/storm-kafka "0.9.0-wip15b-scala292"]
                 [org.clojure/tools.logging "0.2.6"]]
  :main trident-in-clojure.core
  :warn-on-reflection true
  :aot [trident-in-clojure.core
        trident-in-clojure.word-splitter]
  :profiles {:dev 
             {:dependencies [[storm "0.9.0-wip15"]
                             [org.clojure/clojure "1.4.0"]]}}
  :min-lein-version "2.0.0")