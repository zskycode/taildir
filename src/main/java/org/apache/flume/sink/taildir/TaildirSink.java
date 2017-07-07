package org.apache.flume.sink.taildir;

import com.alibaba.fastjson.JSON;
import org.apache.flume.*;
import com.google.common.base.Throwables;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import okhttp3.*;
import com.alibaba.fastjson.JSONArray;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhuzhigang on 2017/7/6.
 */
public class TaildirSink extends AbstractSink implements Configurable {
    private Logger LOG = LoggerFactory.getLogger(TaildirSink.class);
    //    private String hostname;
//    private String port;
    //    private int batchSize = 1;
    private String postUrl;
    private String logTypeName;
    private String appsystem;
    private String hostname;
    long countnum = 0;
    DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss");

    public TaildirSink() {
        LOG.info("TaildirSink start...");
    }

    @Override
    public void start() {
        InetAddress address = null;
        try {
            address = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        hostname = address.getHostName();
        super.start();
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = null;
        Response response = null;
        try {
            transaction = channel.getTransaction();
            transaction.begin();
            Event event1 = null;
            String message = null;
            event1 = channel.take();
            countnum++;
            if (countnum > 1000) {
                Thread.sleep(20);
                countnum = 0;
            }
            Map<String, Object> event = new ConcurrentHashMap<>();
            if (event1 != null) {//对事件进行处理
                message = new String(event1.getBody());
                if (message.contains("[  100]")) {

                } else {
                    EventManager eventManager = new EventManager();
                    eventManager.init();
                    if (eventManager.handleEvent(message)) {

                        event.put("logTypeName", logTypeName);
                        event.put("reqstr", eventManager.getReqstr());
                        String name = hostname;
                        String ss = "_9600";
                        event.put("port", ss);
                        Map dimensionHash = new ConcurrentHashMap();
                        dimensionHash.put("appsystem", appsystem);
                        dimensionHash.put("hostname", name);
                        dimensionHash.put("appprogramname", name + ss);
                        dimensionHash.put("funcid", eventManager.getFuncid());
                        dimensionHash.put("nodeid", eventManager.getNodeid());
                        dimensionHash.put("orgid", eventManager.getOrgid());
                        dimensionHash.put("operway", eventManager.getOperway());

                        event.put("dimensions", dimensionHash);

                        String fSecond = eventManager.getFmillsecond();
                        String sSecond = eventManager.getsMillSecond();
                        int latency = 0;
                        if (fSecond != null && sSecond != null) {
                            latency = Integer.parseInt(sSecond) - Integer.parseInt(fSecond);
                        }
                        Map measureHash = new ConcurrentHashMap();
                        measureHash.put("latency", latency);
                        event.put("measures", measureHash);
                        message = eventManager.getReqstr() + "  " + eventManager.getAnsStr();
                        Map otherfieldHash = new ConcurrentHashMap();
                        String timestr = eventManager.getBegLogtime();
                        DateTime dateTime = new DateTime(dateFormat.parse(timestr).getTime());
                        event.put("timestamp", dateFormat.parse(timestr).getTime());
                        event.put("offset", event1.getHeaders().get("byteoffset"));//dateFormat.parse(timestr).getTime() + countnum
                        event.put("source", event1.getHeaders().get("file"));
                        otherfieldHash.put("beg_logtime", eventManager.getBegLogtime());
                        otherfieldHash.put("end_logtime", eventManager.getEndLogtime());
                        otherfieldHash.put("fmillsecond", fSecond);
                        otherfieldHash.put("smillsecond", sSecond);
                        otherfieldHash.put("messid", eventManager.getMsgid());
                        otherfieldHash.put("netaddr", eventManager.getNetaddr());
                        otherfieldHash.put("fundid", eventManager.getFundid());
                        otherfieldHash.put("inputtype", eventManager.getInputtype());
                        otherfieldHash.put("custid", eventManager.getCustid());
                        otherfieldHash.put("reqstr", eventManager.getReqstr());
                        otherfieldHash.put("message", message);
                        event.put("normalFields", otherfieldHash);
                    }
                }
            } else {
                result = Status.BACKOFF;
            }
            if (message != null) {
                if (!event.containsKey("reqstr")) {
                    transaction.commit();
                } else if (event.get("reqstr").equals("")) {
                    transaction.commit();
                } else {
//                    LOG.info(postUrl + "提交开始!");
//                    LOG.info(JSON.toJSONString(event));
                    response = postJson(postUrl.trim(), JSON.toJSONString(event));
                    if (response != null && response.isSuccessful()) {
//                        LOG.info(postUrl + "提交成功!");
                        transaction.commit();//通过 commit 机制确保数据不丢失
                    } else {

                    }
                }
            } else {
                transaction.commit();
            }
        } catch (Exception e) {
            try {
                transaction.rollback();
            } catch (Exception e2) {
                LOG.error("Exception in rollback. Rollback might not have been" +
                        "successful.", e2);
            }
            LOG.error("Failed to commit transaction." +
                    "Transaction rolled back.", e);
            Throwables.propagate(e);
        } finally {
            if (transaction != null) {
                transaction.close();
                LOG.debug("close Transaction");
            }
            if (response != null) {
                response.close();
            }
        }
        return result;
    }

    @Override
    public void configure(Context context) {
//        hostname = context.getString("hostname");
//        Preconditions.checkNotNull(hostname, "hostname must be set!!");
//        port = context.getString("port");
//        Preconditions.checkNotNull(port, "port must be set!!");
//        batchSize = context.getInteger("batchSize", 100);
//        Preconditions.checkNotNull(batchSize > 0, "batchSize must be a positive number!!");
        postUrl = context.getString("postUrl").trim();//"http://" + hostname + ":" + port;//+"/路径url";
        logTypeName = context.getString("logTypeName").trim();
        appsystem = context.getString("appsystem").trim();
    }

    /**
     * post请求，json数据为body
     * <p>
     * param url
     * param json
     */
    public Response postJson(String url, String json) {
        OkHttpClient client = new OkHttpClient();
        RequestBody body = RequestBody.create(MediaType.parse("application/json"), json);
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();
        Response response = null;
        try {
            response = client.newCall(request).execute();
            if (!response.isSuccessful()) {
                LOG.info("request was error");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }
}
