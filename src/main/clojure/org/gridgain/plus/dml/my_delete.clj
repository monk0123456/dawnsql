(ns org.gridgain.plus.dml.my-delete
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.init.plus-init-sql :as plus-init-sql]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.tools MyConvertUtil KvSql MyDbUtil)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache SqlType MyLog)
             (org.gridgain.dml.util MyCacheExUtil)
             (cn.plus.model.ddl MySchemaTable)
             (java.util List ArrayList Hashtable Date Iterator)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyDelete
        ; 是否生成 class 的 main 方法
        :main false
        ))

(defn delete-table
    ([lst] (delete-table lst nil [] []))
    ([[f & r] my-set stack lst]
     (if (some? f)
         (cond (and (nil? my-set) (not (my-lexical/is-eq? f "where"))) (recur r my-set (conj stack f) lst)
               (and (nil? my-set) (my-lexical/is-eq? f "where")) (recur r "where" stack lst)
               (not (nil? my-set)) (recur r my-set stack (conj lst f))
               )
         (cond (and (= (count stack) 3) (= (second stack) ".")) {:schema_name (str/lower-case (first stack)) :table_name (str/lower-case (last stack)) :where_lst lst}
               (= (count stack) 1) {:schema_name "" :table_name (str/lower-case (first stack)) :where_lst lst}
               :else
               (throw (Exception. "delete 语句错误，要么是dataSetName.tableName 或者是 tableName"))
               ))))

; 获取名字
(defn get_table_name [lst]
    (if (and (my-lexical/is-eq? (first lst) "DELETE") (my-lexical/is-eq? (second lst) "FROM"))
        (delete-table (rest (rest lst)))))

(defn get_pk_lst [[f & r] dic lst]
    (if (some? f)
        (if (contains? dic f)
            (recur r dic (conj lst {:item_name f :item_type (get dic f)}))
            (recur r dic lst))
        lst))

(defn my_view_db [^Ignite ignite group_id ^String schema_name ^String table_name]
    (get_table_name (my-lexical/get-delete-code ignite schema_name table_name group_id)))

;(defn my_delete_query_sql-0 [^Ignite ignite group_id obj]
;    (if-let [{pk_line :line lst_pk :lst_pk dic :dic} (my-update/get_pk_def_map ignite group_id (str/lower-case (-> obj :schema_name)) (str/lower-case (-> obj :table_name)))]
;        {:schema_name (-> obj :schema_name) :table_name (-> obj :table_name) :args (-> obj :args) :sql (format "select %s from %s.%s where %s" pk_line (-> obj :schema_name) (-> obj :table_name) (my-select/my-array-to-sql (-> obj :where_lst))) :pk_lst (get_pk_lst lst_pk dic [])}))

(defn my_delete_query_sql [^Ignite ignite group_id obj]
    (if-let [{schema_name :schema_name table_name :table_name args :args where_lst :where_lst lst-ast :lst-ast pk :pk} obj]
        (letfn [(get_pk_line [[f & r] lst]
                    (if (some? f)
                        (recur r (conj lst (-> f :column_name)))
                        (if-not (empty? lst)
                            (str/join "," lst)
                            "")))]
            {:schema_name schema_name :table_name table_name :args args :sql (format "select %s from %s.%s where %s" (get_pk_line pk []) schema_name table_name (my-select/my-array-to-sql where_lst)) :lst-ast lst-ast :pk_lst pk}
            )))


;(defn my-authority-0 [^Ignite ignite ^Long group_id lst-sql args-dic]
;    (if-let [{schema_name :schema_name table_name :table_name where_lst :where_lst} (get_table_name lst-sql)]
;        (if (and (my-lexical/is-eq? schema_name "my_meta") (= group_id 0))
;            true
;            (let [[where-lst args] (my-update/my-where-line where_lst args-dic)]
;                (if (and (my-lexical/is-eq? schema_name "my_meta") (> group_id 0))
;                    (throw (Exception. "用户不存在或者没有权限！"))
;                    (if-let [{v_table_name :table_name v_where_lst :where_lst} (my_view_db ignite group_id schema_name table_name)]
;                        (if (my-lexical/is-eq? table_name v_table_name)
;                            {:schema_name schema_name :table_name table_name :args args :where_lst (my-update/merge_where where-lst v_where_lst)})
;                        {:schema_name schema_name :table_name table_name :args args :where_lst where-lst}
;                        ))))))

(defn my-authority [^Ignite ignite group_id lst-sql args-dic]
    (if-let [{schema_name :schema_name table_name :table_name where_lst_source :where_lst} (get_table_name lst-sql)]
        (let [{pk :pk data :data} (.get (.cache ignite "table_ast") (MySchemaTable. schema_name table_name))]
            (let [[where-lst args] (my-update/my-where-line where_lst_source args-dic)]
                (cond (and (my-lexical/is-eq? schema_name "my_meta") (= (first group_id) 0)) (if (contains? plus-init-sql/my-grid-tables-set (str/lower-case table_name))
                                                                                                 (throw (Exception. (format "%s 没有删除数据的权限！" table_name)))
                                                                                                 {:schema_name schema_name :table_name table_name :args args :where_lst where-lst :lst-ast (my-update/where-pk pk where_lst_source) :pk pk})
                      (= (first group_id) 0) (if (and (or (my-lexical/is-eq? schema_name "my_meta") (my-lexical/is-str-empty? schema_name)) (contains? plus-init-sql/my-grid-tables-set (str/lower-case table_name)))
                                                 (throw (Exception. (format "%s 没有删除数据的权限！" table_name)))
                                                 (if (my-lexical/is-str-not-empty? schema_name)
                                                     {:schema_name schema_name :table_name table_name :args args :where_lst where-lst :lst-ast (my-update/where-pk pk where_lst_source) :pk pk}
                                                     {:schema_name (second group_id) :table_name table_name :args args :where_lst where-lst :lst-ast (my-update/where-pk pk where_lst_source) :pk pk}))
                      (and (my-lexical/is-eq? schema_name "my_meta") (> (first group_id) 0)) (throw (Exception. "用户不存在或者没有权限！删除数据！"))
                      (and (my-lexical/is-str-empty? schema_name) (my-lexical/is-str-not-empty? (second group_id))) (if-let [{v_table_name :table_name v_where_lst :where_lst} (my_view_db ignite group_id (second group_id) table_name)]
                                                                                                                        (if (my-lexical/is-eq? table_name v_table_name)
                                                                                                                            {:schema_name (second group_id) :table_name table_name :args args :where_lst (my-update/merge_where where-lst v_where_lst) :lst-ast (my-update/where-pk pk (apply concat (map my-lexical/to-back (my-update/merge_where where_lst_source v_where_lst)))) :pk pk})
                                                                                                                        {:schema_name (second group_id) :table_name table_name :args args :where_lst where-lst :lst-ast (my-update/where-pk pk where_lst_source) :pk pk}
                                                                                                                        )
                      (and (my-lexical/is-eq? schema_name (second group_id)) (my-lexical/is-str-not-empty? (second group_id))) (if-let [{v_table_name :table_name v_where_lst :where_lst} (my_view_db ignite group_id schema_name table_name)]
                                                                                                                                   (if (my-lexical/is-eq? table_name v_table_name)
                                                                                                                                       {:schema_name schema_name :table_name table_name :args args :where_lst (my-update/merge_where where-lst v_where_lst) :lst-ast (my-update/where-pk pk (apply concat (map my-lexical/to-back (my-update/merge_where where_lst_source v_where_lst)))) :pk pk})
                                                                                                                                   {:schema_name schema_name :table_name table_name :args args :where_lst where-lst :lst-ast (my-update/where-pk pk where-lst) :pk pk}
                                                                                                                                   )
                      (and (not (my-lexical/is-eq? schema_name (second group_id))) (my-lexical/is-str-not-empty? schema_name) (my-lexical/is-str-not-empty? (second group_id))) (if-let [{v_table_name :table_name v_where_lst :where_lst} (my_view_db ignite group_id schema_name table_name)]
                                                                                                                                                                                    (if (my-lexical/is-eq? table_name v_table_name)
                                                                                                                                                                                        {:schema_name schema_name :table_name table_name :args args :where_lst (my-update/merge_where where-lst v_where_lst) :lst-ast (my-update/where-pk pk (apply concat (map my-lexical/to-back (my-update/merge_where where_lst_source v_where_lst)))) :pk pk})
                                                                                                                                                                                    (if (not (my-lexical/is-eq? schema_name "public"))
                                                                                                                                                                                        (throw (Exception. "用户不存在或者没有权限！删除数据！"))
                                                                                                                                                                                        {:schema_name "public" :table_name table_name :args args :where_lst where-lst :lst-ast (my-update/where-pk pk where_lst_source) :pk pk})
                                                                                                                                                                                    )
                      )))
        ))

;(defn my-no-authority-0 [^Ignite ignite ^Long group_id lst-sql args-dic]
;    (if-let [{schema_name :schema_name table_name :table_name where_lst :where_lst} (get_table_name lst-sql)]
;        (let [[where-lst args] (my-update/my-where-line where_lst args-dic)]
;            (if (and (my-lexical/is-eq? schema_name "my_meta") (> group_id 0))
;                (throw (Exception. "用户不存在或者没有权限！"))
;                {:schema_name schema_name :table_name table_name :args args :where_lst where-lst}))
;        ))

(defn my-no-authority [^Ignite ignite group_id lst-sql args-dic]
    (if-let [{schema_name :schema_name table_name :table_name where_lst :where_lst} (get_table_name lst-sql)]
        (let [[where-lst args] (my-update/my-where-line where_lst args-dic)]
            (cond (and (my-lexical/is-eq? schema_name "my_meta") (> (first group_id) 0)) (throw (Exception. "用户不存在或者没有权限！"))
                  :else
                  {:schema_name schema_name :table_name table_name :args args :where_lst where-lst}))
        ))

;(defn my_delete_obj-0 [^Ignite ignite ^Long group_id lst-sql args-dic]
;    (if-let [m (my-authority ignite group_id lst-sql args-dic)]
;        (if (and (boolean? m) (true? m))
;            true
;            (my_delete_query_sql ignite group_id m))
;        (throw (Exception. "删除语句字符串错误！"))))

(defn my_delete_obj [^Ignite ignite group_id lst-sql args-dic]
    (if-let [m (my-lexical/get-re-obj ignite (my-authority ignite group_id lst-sql args-dic))]
        (if-let [ms (my_delete_query_sql ignite group_id m)]
            (if (not (= (count (-> ms :lst-ast)) (count (-> ms :pk_lst))))
                (assoc ms :lst-ast nil)
                ms))
        (throw (Exception. "删除语句字符串错误！"))))

;(defn my_delete_obj-no-authority-0 [^Ignite ignite ^Long group_id lst-sql args-dic]
;    (if-let [m (my-no-authority ignite group_id lst-sql args-dic)]
;        (if (and (boolean? m) (true? m))
;            true
;            (my_delete_query_sql ignite group_id m))
;        (throw (Exception. "删除语句字符串错误！"))))

(defn my_delete_obj-no-authority [^Ignite ignite group_id lst-sql args-dic]
    (if-let [m (my-lexical/get-re-obj ignite (my-no-authority ignite group_id lst-sql args-dic))]
        (if-let [ms (my_delete_query_sql ignite group_id m)]
            (if (not (= (count (-> ms :lst-ast)) (count (-> ms :pk_lst))))
                (assoc ms :lst-ast nil)
                ms))
        (throw (Exception. "删除语句字符串错误！"))))