package org.apache.flume.sink.taildir;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhuzhigang on 2017/7/6.
 */
public class CacheManager {
    static Map<String, Map<String, Kcbplog>> Cache = new ConcurrentHashMap();
    static Map<String, Map<String, Kcbplog>> CacheAns = new ConcurrentHashMap();

    public void addThreadIdMsgiD(String threadId, String msgId, String time, String valuestr, String millTime) {
        if (Cache.containsKey(threadId)) {
            if (Cache.get(threadId).containsKey(msgId)) {
                Kcbplog kcbpmodel = new Kcbplog();
                kcbpmodel.setReq(valuestr.trim());
                kcbpmodel.setTime(time);
                kcbpmodel.setMillTime(millTime);
                Map<String, Kcbplog> tem = Cache.get(threadId);
                tem.put(msgId, kcbpmodel);
                Cache.put(threadId, tem);
            } else {
                Map<String, Kcbplog> temCache = new ConcurrentHashMap();
                Kcbplog kcbpmodel = new Kcbplog();
                kcbpmodel.setReq(valuestr.trim());
                kcbpmodel.setTime(time);
                kcbpmodel.setMillTime(millTime);
                temCache.put(msgId, kcbpmodel);
                Cache.put(threadId, temCache);
            }
        } else {
            Map<String, Kcbplog> temCache = new ConcurrentHashMap();
            Kcbplog kcbpmodel = new Kcbplog();
            kcbpmodel.setReq(valuestr.trim());
            kcbpmodel.setTime(time);
            kcbpmodel.setMillTime(millTime);
            temCache.put(msgId, kcbpmodel);
            Cache.put(threadId, temCache);
        }
    }

    public void addThreadIdMsgiDAns(String threadId, String msgId, String time, String valuestr, String millTime) {
        if (CacheAns.containsKey(threadId)) {
            if (CacheAns.get(threadId).containsKey(msgId)) {

                Kcbplog kcbpmodel = new Kcbplog();
                kcbpmodel.setRep(valuestr.trim());
                kcbpmodel.setTime(time);
                kcbpmodel.setMillTime(millTime);
                Map<String, Kcbplog> tem = CacheAns.get(threadId);
                tem.put(msgId, kcbpmodel);
                CacheAns.put(threadId, tem);
            } else {
                Map<String, Kcbplog> temCache = new ConcurrentHashMap();
                Kcbplog kcbpmodel = new Kcbplog();
                kcbpmodel.setRep(valuestr.trim());
                kcbpmodel.setTime(time);
                kcbpmodel.setMillTime(millTime);
                temCache.put(msgId, kcbpmodel);
                CacheAns.put(threadId, temCache);
            }
        } else {
            Map<String, Kcbplog> temCache = new ConcurrentHashMap();
            Kcbplog kcbpmodel = new Kcbplog();
            kcbpmodel.setRep(valuestr.trim());
            kcbpmodel.setTime(time);
            kcbpmodel.setMillTime(millTime);
            temCache.put(msgId, kcbpmodel);
            CacheAns.put(threadId, temCache);
        }
    }

    public Kcbplog find(String msgId) {
        for (String k : Cache.keySet()) {
            if (Cache.get(k).containsKey(msgId)) {
                Kcbplog kcbpm = Cache.get(k).get(msgId);
                return kcbpm;
            }
        }
        return null;
    }


    public Kcbplog findAns(String threadId) {
        if (CacheAns.containsKey(threadId)) {
            for (String k : CacheAns.get(threadId).keySet()) {
                Kcbplog kcbpm = CacheAns.get(threadId).get(k);
                if (kcbpm != null) {
                    return kcbpm;
                }

            }
            return null;
        }
        return null;
    }


    public void delete(String msgId) {
        for (String k : Cache.keySet()) {
            if (Cache.get(k).containsKey(msgId)) {
                Cache.remove(k);
            }
        }
    }

    public void deleteAns(String threadId) {
        for (String k : CacheAns.keySet()) {
            if (k.equals(threadId)) {
                CacheAns.remove(threadId);
            }
        }
    }

    public void appendkey(String threadId, String value) {
        if (Cache.containsKey(threadId)) {
            for (String k : Cache.get(threadId).keySet()) {
                Kcbplog kcbpm = Cache.get(threadId).get(k);
                if (kcbpm != null) {
                    kcbpm.setReq(kcbpm.getReq() + value.trim());
                    Map<String, Kcbplog> tem = Cache.get(threadId);
                    tem.put(k, kcbpm);
                    Cache.put(threadId, tem);
                    break;
                }
            }
        }
    }


    public void appendkeyAns(String threadId, String value) {
        if (CacheAns.containsKey(threadId)) {
            for (String k : CacheAns.get(threadId).keySet()) {
                Kcbplog kcbpm = CacheAns.get(threadId).get(k);
                if (kcbpm != null) {
                    kcbpm.setRep(kcbpm.getRep() + value.trim());
                    Map<String, Kcbplog> tem = CacheAns.get(threadId);
                    tem.put(k, kcbpm);
                    CacheAns.put(threadId, tem);
                    break;
                }
            }
        }
    }
}