(ns org.gridgain.plus.dml.my-func-ast
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-smart-sql :as my-smart-sql]
        [org.gridgain.plus.dml.my-smart-db :as my-smart-db]
        [org.gridgain.plus.dml.my-smart-token-clj :as my-smart-token-clj]
        [org.gridgain.plus.dml.my-select-plus :as my-select-plus]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [org.gridgain.plus.ml.my-ml-train-data :as my-ml-train-data]
        [clojure.core.reducers :as r]
        [clojure.string :as str]
        [clojure.walk :as w])
    (:import (org.apache.ignite Ignite)
             (org.gridgain.smart MyVar MyLetLayer)
             (com.google.common.base Strings)
             (com.google.gson Gson GsonBuilder)
             (cn.plus.model MyKeyValue MyLogCache SqlType)
             (cn.plus.model.db MyScenesCache ScenesType MyScenesParams MyScenesParamsPk MyScenesCachePk)
             (org.gridgain.dml.util MyCacheExUtil)
             (org.apache.ignite.cache.query SqlFieldsQuery)
             (java.math BigDecimal)
             (org.log MyCljLogger)
             (java.util List ArrayList Hashtable Date Iterator)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyFuncAst
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [get_plus_sql [org.apache.ignite.Ignite Long String] String]
        ;          ^:static [getSqlToAst [org.apache.ignite.Ignite String String] clojure.lang.LazySeq]
        ;          ^:static [putAstCache [org.apache.ignite.Ignite String String String] void]]
        ))

(declare sql-ast my-func-ast-items)

(defn insert-ast [ignite group_id insert-ast-obj]
    (letfn [(get-ast-map-items [ast-map]
                (cond (contains? ast-map :item_value) (sql-ast ignite group_id (-> ast-map :item_value))
                      :else
                      (loop [[f & r] (keys ast-map) rs []]
                          (if (some? f)
                              (if-let [f-m (get-ast-items (get ast-map f))]
                                  (if (my-lexical/is-seq? f-m)
                                      (recur r (apply conj rs f-m))
                                      (recur r (conj rs f-m)))
                                  (recur r rs))
                              rs))
                      ))
            (get-ast-lst-items
                ([lst] (get-ast-lst-items lst []))
                ([[f & r] lst]
                 (if (some? f)
                     (if-let [f-m (get-ast-items f)]
                         (if (my-lexical/is-seq? f-m)
                             (recur r (apply conj lst f-m))
                             (recur r (conj lst f-m)))
                         (recur r lst))
                     lst)))
            (get-ast-items [ast]
                (cond (map? ast) (get-ast-map-items ast)
                      (my-lexical/is-seq? ast) (get-ast-lst-items ast)
                      ))]
        (get-ast-items insert-ast-obj)))

(defn is-items [other-ast]
    (cond (map? other-ast) (if (contains? other-ast :item_name)
                               true false)
          (my-lexical/is-seq? other-ast) (if (= (count other-ast) 1)
                                             (is-items (first other-ast))
                                             false)
          ))

(defn sql-ast [ignite group_id lst]
    (cond (my-lexical/is-eq? "select" (first lst)) (if-let [ast (my-select-plus/sql-to-ast lst)]
                                                       (my-func-ast-items ignite group_id ast))
          (my-lexical/is-eq? "insert" (first lst)) (let [insert-ast-obj (my-insert/my_insert_obj ignite group_id lst)]
                                                       (my-func-ast-items ignite group_id (insert-ast ignite group_id insert-ast-obj)))
          (my-lexical/is-eq? "update" (first lst)) (let [{items :items where_line :where_line} (my-update/my-authority ignite group_id lst nil)]
                                                       (concat (my-func-ast-items ignite group_id items) (my-func-ast-items ignite group_id (my-select-plus/sql-to-ast where_line))))
          (my-lexical/is-eq? "delete" (first lst)) (let [{where_lst :where_lst} (my-delete/my-authority ignite group_id lst nil)]
                                                       (my-func-ast-items ignite group_id (my-select-plus/sql-to-ast where_lst)))
          :else
          (let [other-ast (my-select-plus/sql-to-ast lst)]
              (if (false? (is-items other-ast))
                  (my-func-ast-items ignite group_id other-ast)))
          ))

; 获取 ast 中 item
(defn my-func-ast-items [ignite group_id ast]
    (letfn [(has-item-in [[f & r] item]
                (if (some? f)
                    (if (my-lexical/is-eq? (-> f :func-name) (-> item :func-name))
                        true
                        (recur r item))
                    false))
            (get-only-item
                ([lst] (get-only-item lst []))
                ([[f & r] lst-only]
                 (if (some? f)
                     (if (has-item-in lst-only f)
                         (recur r lst-only)
                         (recur r (conj lst-only f)))
                     lst-only)))
            (get-ast-map-items [ast-map]
                (cond (and (contains? ast-map :func-name) (not (contains? ast-map :body-lst))) ast-map
                      (and (contains? ast-map :item_name) (= java.lang.String (-> ast-map :java_item_type))) (sql-ast ignite group_id (my-lexical/to-back (my-lexical/get_str_value (-> ast-map :item_name))))
                      :else
                      (loop [[f & r] (keys ast-map) rs []]
                          (if (some? f)
                              (if-let [f-m (get-ast-items (get ast-map f))]
                                  (if (my-lexical/is-seq? f-m)
                                      (recur r (apply conj rs f-m))
                                      (recur r (conj rs f-m)))
                                  (recur r rs))
                              rs))
                      ))
            (get-ast-lst-items
                ([lst] (get-ast-lst-items lst []))
                ([[f & r] lst]
                 (if (some? f)
                     (if-let [f-m (get-ast-items f)]
                         (if (my-lexical/is-seq? f-m)
                             (recur r (apply conj lst f-m))
                             (recur r (conj lst f-m)))
                         (recur r lst))
                     lst)))
            (get-ast-items [ast]
                (cond (map? ast) (get-ast-map-items ast)
                      (my-lexical/is-seq? ast) (get-ast-lst-items ast)
                      ))]
        (if-let [items-lst (get-ast-items ast)]
            (cond (map? items-lst) [items-lst]
                  (my-lexical/is-seq? items-lst) (cond (= (count items-lst) 1) items-lst
                                                       (> (count items-lst) 1) (get-only-item (get-ast-items ast)))
                  ))
        ))

(defn get-func-name [lst]
    (loop [[f & r] lst rs []]
        (if (some? f)
            (recur r (conj rs (-> f :func-name)))
            rs)))

; 获取所有的 func
(defn get-func-code [^Ignite ignite group_id ^String func-name]
    (if-let [m (.get (.cache ignite "my_scenes") (MyScenesCachePk. (first group_id) func-name))]
        (let [ast (my-smart-sql/get-ast-lst (my-lexical/to-back (.getSmart_code m))) my-rs [(.getSmart_code m)]]
            (loop [[f & r] (get-func-name (my-func-ast-items ignite group_id ast)) rs []]
                (if (some? f)
                    (recur r (concat rs (get-func-code ignite group_id f)))
                    (concat my-rs rs))))))
























































