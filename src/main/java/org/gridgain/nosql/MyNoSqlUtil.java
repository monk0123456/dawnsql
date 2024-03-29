package org.gridgain.nosql;

import cn.plus.model.CacheDllType;
import cn.plus.model.MyCacheEx;
import cn.plus.model.MyNoSqlCache;
import cn.plus.model.MySmartCache;
import cn.plus.model.ddl.MyCachePK;
import cn.plus.model.ddl.MyCaches;
import cn.smart.service.IMyLogTrans;
import com.google.common.base.Strings;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicyFactory;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.smart.service.MyLogService;
import org.apache.ignite.transactions.Transaction;
import org.gridgain.dml.util.MyCacheExUtil;
import org.gridgain.smart.ml.model.MyMModelKey;
import org.gridgain.smart.ml.model.MyMlModel;
import org.tools.MyConvertUtil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * No sql
 * */
public class MyNoSqlUtil {

    private static IMyLogTrans myLog = MyLogService.getInstance().getMyLog();

    public static CacheConfiguration getCfg(final String cacheName)
    {
        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>();
        cfg.setCacheMode(CacheMode.REPLICATED);
        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cfg.setName(cacheName);
        cfg.setReadFromBackup(true);

        return cfg;
    }

    public static CacheConfiguration getCfg(final String cacheName, final String dataRegin)
    {
        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>();
        cfg.setCacheMode(CacheMode.REPLICATED);
        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cfg.setDataRegionName(dataRegin);
        cfg.setName(cacheName);
        cfg.setReadFromBackup(true);

        return cfg;
    }

    public static CacheConfiguration getNearCfg(final String schema_name, final String cacheName, final String mode, final int maxSize, final int backups)
    {
        NearCacheConfiguration<Object, Object> nearCfg = new NearCacheConfiguration<>();

        // Use LRU eviction policy to automatically evict entries
        // from near-cache whenever it reaches 100_00 entries
        if (maxSize > 0) {
            nearCfg.setNearEvictionPolicyFactory(new LruEvictionPolicyFactory<>(maxSize));
        }
        else
        {
            nearCfg.setNearEvictionPolicyFactory(new LruEvictionPolicyFactory<>());
        }

        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>();
        if (!Strings.isNullOrEmpty(mode) && mode.toLowerCase().equals("replicated")) {
            cfg.setCacheMode(CacheMode.REPLICATED);
        }
        else
        {
            cfg.setCacheMode(CacheMode.PARTITIONED);
            if (backups > 0) {
                cfg.setBackups(backups);
            }
        }
        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        if (!Strings.isNullOrEmpty(schema_name) && schema_name.toLowerCase().equals("my_meta")) {
            cfg.setDataRegionName("MySys_Caches_Region_Eviction");
        }
        else
        {
            cfg.setDataRegionName("Near_Caches_Region_Eviction");
        }
        cfg.setName(cacheName);
        cfg.setReadFromBackup(true);
        cfg.setNearConfiguration(nearCfg);

        return cfg;
    }

    public static CacheConfiguration getCacheCfg(final String cacheName, final String mode, final int backups)
    {

        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>();
        if (!Strings.isNullOrEmpty(mode) && mode.toLowerCase().equals("replicated")) {
            cfg.setCacheMode(CacheMode.REPLICATED);
        }
        else
        {
            cfg.setCacheMode(CacheMode.PARTITIONED);
            if (backups > 0) {
                cfg.setBackups(backups);
            }
        }
        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cfg.setName(cacheName);
        cfg.setReadFromBackup(true);

        return cfg;
    }

    public static void createCache(final Ignite ignite, final String ds_name, final String cacheName, final Boolean is_cache, final String mode, final int maxSize, final int backups)
    {
        CacheConfiguration configuration;
        if (is_cache == true)
        {
            configuration = getNearCfg(ds_name, cacheName, mode, maxSize, backups);
        }
        else
        {
            configuration = getCacheCfg(cacheName, mode, backups);
        }

        if (myLog != null)
        {
            configuration.setSqlSchema(ds_name);
            ignite.getOrCreateCache(configuration);

            IgniteTransactions transactions = ignite.transactions();
            Transaction tx = null;
            String transSession = UUID.randomUUID().toString();
            try {
                tx = transactions.txStart();
                myLog.createSession(transSession);

                MySmartCache mySmartCache = new MySmartCache();
                mySmartCache.setCacheDllType(CacheDllType.CREATE);
                mySmartCache.setIs_cache(is_cache);
                mySmartCache.setMode(mode);
                if (maxSize > 0) {
                    mySmartCache.setMaxSize(maxSize);
                }

                if (backups > 0)
                {
                    mySmartCache.setBackups(backups);
                }
                mySmartCache.setTable_name(cacheName);

                myLog.saveTo(transSession, MyCacheExUtil.objToBytes(mySmartCache));

                myLog.commit(transSession);
                tx.commit();
            } catch (Exception ex) {
                if (tx != null) {
                    myLog.rollback(transSession);
                    tx.rollback();
                    ignite.destroyCache(cacheName);
                }
            } finally {
                if (tx != null) {
                    tx.close();
                }
            }
        }
        else
        {
            ignite.getOrCreateCache(configuration);
        }
    }

    public static void createCacheSave(final Ignite ignite, final String schema_name, final String table_name, final Boolean is_cache, final String mode, final int maxSize, final int backups) throws Exception {
        String cacheName = "c_" + schema_name.toLowerCase() + "_" + table_name.toLowerCase();
        CacheConfiguration configuration;
        if (is_cache == true)
        {
            configuration = getNearCfg(schema_name, cacheName, mode, maxSize, backups);
        }
        else
        {
            configuration = getCacheCfg(cacheName, mode, backups);
        }

        if (myLog != null)
        {
            configuration.setSqlSchema(schema_name);
            IgniteCache<MyCachePK, MyCaches> my_caches = ignite.cache("my_caches");
            MyCachePK key = new MyCachePK(schema_name, table_name);
            if (!my_caches.containsKey(key)) {
                ignite.getOrCreateCache(configuration);

                IgniteTransactions transactions = ignite.transactions();
                Transaction tx = null;
                String transSession = UUID.randomUUID().toString();
                try {
                    tx = transactions.txStart();
                    myLog.createSession(transSession);

                    MySmartCache mySmartCache = new MySmartCache();
                    mySmartCache.setCacheDllType(CacheDllType.CREATE);
                    mySmartCache.setIs_cache(is_cache);
                    mySmartCache.setMode(mode);
                    if (maxSize > 0) {
                        mySmartCache.setMaxSize(maxSize);
                    }

                    if (backups > 0) {
                        mySmartCache.setBackups(backups);
                    }
                    mySmartCache.setTable_name(cacheName);

                    myLog.saveTo(transSession, MyCacheExUtil.objToBytes(mySmartCache));

                    my_caches.put(key, new MyCaches(schema_name, table_name, is_cache, mode, maxSize, backups));

                    myLog.commit(transSession);
                    tx.commit();
                } catch (Exception ex) {
                    if (tx != null) {
                        myLog.rollback(transSession);
                        tx.rollback();
                        ignite.destroyCache(cacheName);
                    }
                } finally {
                    if (tx != null) {
                        tx.close();
                    }
                }
            }
            else
            {
                throw new Exception(table_name + "已经存在，不能新建！");
            }
        }
        else
        {
            IgniteCache<MyCachePK, MyCaches> my_caches = ignite.cache("my_caches");
            MyCachePK key = new MyCachePK(schema_name, table_name);
            if (!my_caches.containsKey(key)) {
                ignite.getOrCreateCache(configuration);
                my_caches.put(key, new MyCaches(schema_name, table_name, is_cache, mode, maxSize, backups));
            }
            else
            {
                throw new Exception(table_name + "已经存在，不能新建！");
            }
        }
    }

    public static void dropCache(final Ignite ignite, final String cacheName) throws Exception {
        if (myLog != null)
        {
            MySmartCache mySmartCache = new MySmartCache();
            mySmartCache.setCacheDllType(CacheDllType.DROP);
            mySmartCache.setTable_name(cacheName);

            String transSession = UUID.randomUUID().toString();
            try
            {
                myLog.createSession(transSession);
                myLog.saveTo(transSession, MyCacheExUtil.objToBytes(mySmartCache));
                ignite.destroyCache(cacheName);
                myLog.commit(transSession);

            }
            catch (Exception ex)
            {
                myLog.rollback(transSession);
            }
        }
        else
        {
            ignite.destroyCache(cacheName);
        }
    }

    public static void dropCacheSave(final Ignite ignite, final String schema_name, final String table_name) throws Exception {
        String cacheName = "c_" + schema_name.toLowerCase() + "_" + table_name.toLowerCase();
        if (myLog != null)
        {
            MySmartCache mySmartCache = new MySmartCache();
            mySmartCache.setCacheDllType(CacheDllType.DROP);
            mySmartCache.setTable_name(cacheName);

            String transSession = UUID.randomUUID().toString();
            try
            {
                myLog.createSession(transSession);
                myLog.saveTo(transSession, MyCacheExUtil.objToBytes(mySmartCache));
                ignite.destroyCache(cacheName);
                myLog.commit(transSession);

            }
            catch (Exception ex)
            {
                myLog.rollback(transSession);
            }
        }
        else
        {
            ignite.destroyCache(cacheName);
        }
    }

    public static void runCache(final Ignite ignite, final MyNoSqlCache myNoSqlCache) throws Exception {
        List<MyNoSqlCache> lst = new ArrayList<>();
        lst.add(myNoSqlCache);
        MyCacheExUtil.transLogCache(ignite, lst);
    }

    public static void initCaches(final Ignite ignite)
    {
        CacheConfiguration<MyMModelKey, MyMlModel> cfg = new CacheConfiguration<>();
        cfg.setName("my_ml_model");
        cfg.setCacheMode(CacheMode.PARTITIONED);

        ignite.getOrCreateCache(cfg);

        SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery("select m.schema_name, m.table_name, m.is_cache, m.mode, m.maxSize, m.backups from MY_META.my_caches m");
        sqlFieldsQuery.setLazy(true);
        Iterator<List<?>> iterator = ignite.cache("my_caches").query(sqlFieldsQuery).iterator();
        while (iterator.hasNext())
        {
            List<?> row = iterator.next();
            String cache_name = "c_" + row.get(0).toString().toLowerCase() + "_" + row.get(1).toString().toLowerCase();
            MyNoSqlUtil.createCache(ignite, row.get(0).toString(), cache_name, MyConvertUtil.ConvertToBoolean(row.get(2)), row.get(3).toString(), MyConvertUtil.ConvertToInt(row.get(4)), MyConvertUtil.ConvertToInt(row.get(5)));
            System.out.println(cache_name + " 初始化成功！");
        }
    }

//    public static void defineCache(final Ignite ignite, final Long group_id, final String cacheName, final String dataRegin, final String line)
//    {
//        ignite.getOrCreateCache(getCfg(cacheName, dataRegin));
//        IgniteCache<MyCacheGroup, MyCacheValue> cache = ignite.cache("my_cache");
//        cache.put(new MyCacheGroup(cacheName, group_id), new MyCacheValue(line, dataRegin));
//    }
//
//    public static Boolean hasCache(final Ignite ignite, final String cacheName, final Long group_id)
//    {
//        Boolean rs = ignite.cache("my_cache").containsKey(new MyCacheGroup(cacheName, group_id));
//        if (rs)
//        {
//            return true;
//        }
//        return ignite.cache("my_cache").containsKey(new MyCacheGroup(cacheName, 0L));
//    }
//
//    public static void destroyCache(final Ignite ignite, final String cacheName, final Long group_id)
//    {
//        if (hasCache(ignite, cacheName, group_id)) {
//            ignite.cache("my_cache").remove(new MyCacheGroup(cacheName, group_id));
//            ignite.cache(cacheName).destroy();
//        }
//    }
}
