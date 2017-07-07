package org.apache.flume.sink.taildir;

import com.alibaba.fastjson.JSON;
import okhttp3.*;
import org.joda.time.DateTime;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhuzhigang on 2017/7/6.
 */
public class Main {
    public static void main(String[] args) throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss");
        Map<String, Object> event = new ConcurrentHashMap<>();
        try {
            String encoding = "GBK";
            File file = new File("/Users/zhuzhigang/Downloads/apache-flume-1.7.0-bin/test/kcbpdata.log");
            if (file.isFile() && file.exists()) { //判断文件是否存在
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file), encoding);//考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String message = null;
                while ((message = bufferedReader.readLine()) != null) {
                    if (message.contains("[  100]")) {

                    } else {
                        EventManager eventManager = new EventManager();
                        eventManager.init();
                        if (eventManager.handleEvent(message)) {

                            event.put("logTypeName", "");
                            event.put("reqstr", eventManager.getReqstr());
                            String name = "";
                            String ss = "_9600";
                            event.put("port", ss);
                            Map dimensionHash = new ConcurrentHashMap();
                            dimensionHash.put("appsystem", "");
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
                            event.put("offset", dateFormat.parse(timestr).getTime() + 1);
                            event.put("source", "");
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

                    if (message != null) {
                        if (!event.containsKey("reqstr")) {
//                    transaction.commit();
                        } else if (event.get("reqstr").equals("")) {
//                    transaction.commit();
                        } else {
//                            System.out.println("提交开始!");
                            Response response = postJson("http://127.0.0.1:10007", JSON.toJSONString(event));
                            if (response != null && response.isSuccessful()) {
//                                 System.out.println("提交成功!");
//                                System.out.print(response);
                            }
                        }
                    }
                }
                read.close();
            } else {
                System.out.println("找不到指定的文件");
            }
        } catch (Exception e) {
            System.out.println("读取文件内容出错");
            e.printStackTrace();
        }


    }

    /**
     * post请求，json数据为body
     * <p>
     * param url
     * param json
     */
    public static Response postJson(String url, String json) {
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
//                LOG.info("request was error");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }
}
