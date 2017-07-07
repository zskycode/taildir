package org.apache.flume.sink.taildir;

/**
 * Created by zhuzhigang on 2017/7/6.
 */
public class EventManager {
    CacheManager cacheManager = null;
    String dateTime = null;
    String sMillSecond = null;
    String sThreadid = null;
    String sLevel = null;
    String remainString = null;
    String msgid = null;
    String endLogtime = null;
    String smillsecond = null;
    String AnsStr = null;
    String reqstr = null;
    String begLogtime = null;
    String fmillsecond = null;
    String nodeid = null;
    String custid = null;
    String funcid = null;
    String netaddr = null;
    String orgid = null;
    String operway = null;
    String fundid = null;
    String inputtype = null;
    int flagstr = 0;

    public void init() {
        cacheManager = new CacheManager();
    }

    public void splitString(String event) {

        String a[] = event.split("]");
        if (a.length == 5 && event.trim().startsWith("[20")) {
            this.dateTime = a[0].split("\\[")[1].trim();
            this.sMillSecond = a[1].split("\\[")[1].trim();
            this.sThreadid = a[2].split("\\[")[1].trim();
            this.sLevel = a[3].split("\\[")[1].trim();
            this.remainString = a[4];
        } else {
            flagstr = 1;
            this.dateTime = null;
            this.sMillSecond = null;
            this.sThreadid = null;
            this.sLevel = null;
            this.remainString = null;
        }
    }

    public String getValue(String key, String symbol, String str) {
        if (str != null && !str.equals("")) {
            if (str.contains(key)) {
                String str11 = str.split(key)[1].split(symbol)[0];
                if (str11 != null && !str11.equals("")) {
                    str = str11;
                }
                return str;
            } else {
                return "";
            }
        }
        return "";
    }

    public boolean handleEvent(String event) {
        sThreadid = "";
        msgid = "";
        dateTime = "";
        remainString = "";
        sMillSecond = "";
        funcid = "";
        fmillsecond = "";
        inputtype = "";
        custid = "";
        netaddr = "";
        reqstr = "";
        begLogtime = "";
        endLogtime = "";
        funcid = "";
        fundid = "";
        operway = "";
        orgid = "";
        smillsecond = "";
        splitString(event);
        if (flagstr == 0) {
            if (remainString.contains("Req:")) {
                handleReqHeader();
            } else if (remainString.contains("Ans:")) {
                handleAnsHeader();
            } else if (sLevel.contains("99")) {
                handleReqParts();
            } else if (sLevel.contains("98")) {
                return handleAnsParts();
            } else {
                handleOthers();
            }
        }
        return false;
    }

    public boolean handleReqHeader() {
        this.msgid = getValue("MsgId=", ",", remainString);
        cacheManager.addThreadIdMsgiD(sThreadid, msgid, dateTime, remainString, sMillSecond);
        return false;
    }

    public boolean handleAnsHeader() {
        this.msgid = getValue("MsgId=", ",", remainString);
        cacheManager.addThreadIdMsgiDAns(sThreadid, msgid, dateTime, remainString, sMillSecond);
        return false;
    }

    public boolean handleReqParts() {
        cacheManager.appendkey(sThreadid, remainString);
        return false;
    }

    public boolean handleAnsParts() {
        cacheManager.appendkeyAns(sThreadid, remainString);
        this.endLogtime = dateTime;
        this.smillsecond = smillsecond;
        Kcbplog a = cacheManager.findAns(sThreadid);
        if (a == null || a.equals("")) {
            return false;
        }
        if (a != null) {
            AnsStr = a.getRep();
            endLogtime = a.getTime();
            smillsecond = a.getMillTime();
        }
        if (AnsStr != "" || AnsStr != null) {
            int len = 0;
            if (AnsStr.contains("Len=")) {
                len = Integer.parseInt(getValue("Len=", ",", AnsStr));
            }
            int length = AnsStr.split("Buf=")[1].length();
            if (length >= len || remainString.length() < 70) {
                msgid = getValue("MsgId=", ",", AnsStr);
                Kcbplog tmp = cacheManager.find(msgid);
                if (tmp == null || tmp.equals("")) {
                    return false;
                }
                if (tmp != null) {
                    reqstr = tmp.getReq();
                    begLogtime = tmp.getTime();
                    fmillsecond = tmp.getMillTime();
                }
                if (null != reqstr || !reqstr.equals("")) {
                    cacheManager.delete(msgid);
                    cacheManager.deleteAns(sThreadid);
                    return changeStrToEvent(msgid);
                }
            }

        }
        return false;
    }

    public boolean handleOthers() {
        return false;
    }

    private boolean changeStrToEvent(String msgid) {
        if (!msgid.equals("") && !reqstr.equals("")) {
            nodeid = getValue("NodeId=", ",", reqstr);
            custid = getValue("F_OP_USER=", "&", reqstr);
            if (custid.equals("")) {
                custid = getValue("custid=", "&", reqstr);
            }
            funcid = getValue("F_FUNCTION=", "&", reqstr);
            if (funcid.equals("")) {
                funcid = getValue("request=", "|", reqstr);
            }
            if (funcid.equals("")) {
                funcid = getValue("funcid=", "&", reqstr);
            }
            if (funcid == "") {
                funcid = "99999999";
            }
            netaddr = getValue("F_OP_SIZE=", "&", reqstr);
            if (netaddr.equals("")) {
                netaddr = getValue("netaddr=", "&", reqstr);
            }
            orgid = getValue("F_OP_ORG=", "&", reqstr);
            if (orgid.equals("")) {
                orgid = getValue("orgid=", "&", reqstr);
            }
            operway = getValue("operway=", "&", reqstr);
            funcid = getValue("fundid=", "&", reqstr);
            inputtype = getValue("ACCT_TYP E=", "&", reqstr);
            if (inputtype.equals("")) {
                inputtype = getValue("inputtype=", "&", reqstr);
            }
            return true;
        } else {
            return false;
        }
    }

    public CacheManager getCacheManager() {
        return cacheManager;
    }

    public void setCacheManager(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    public String getsMillSecond() {
        return sMillSecond;
    }

    public void setsMillSecond(String sMillSecond) {
        this.sMillSecond = sMillSecond;
    }

    public String getsThreadid() {
        return sThreadid;
    }

    public void setsThreadid(String sThreadid) {
        this.sThreadid = sThreadid;
    }

    public String getsLevel() {
        return sLevel;
    }

    public void setsLevel(String sLevel) {
        this.sLevel = sLevel;
    }

    public String getRemainString() {
        return remainString;
    }

    public void setRemainString(String remainString) {
        this.remainString = remainString;
    }

    public String getMsgid() {
        return msgid;
    }

    public void setMsgid(String msgid) {
        this.msgid = msgid;
    }

    public String getEndLogtime() {
        return endLogtime;
    }

    public void setEndLogtime(String endLogtime) {
        this.endLogtime = endLogtime;
    }

    public String getSmillsecond() {
        return smillsecond;
    }

    public void setSmillsecond(String smillsecond) {
        this.smillsecond = smillsecond;
    }

    public String getAnsStr() {
        return AnsStr;
    }

    public void setAnsStr(String ansStr) {
        AnsStr = ansStr;
    }

    public String getReqstr() {
        return reqstr;
    }

    public void setReqstr(String reqstr) {
        this.reqstr = reqstr;
    }

    public String getBegLogtime() {
        return begLogtime;
    }

    public void setBegLogtime(String begLogtime) {
        this.begLogtime = begLogtime;
    }

    public String getFmillsecond() {
        return fmillsecond;
    }

    public void setFmillsecond(String fmillsecond) {
        this.fmillsecond = fmillsecond;
    }

    public String getNodeid() {
        return nodeid;
    }

    public void setNodeid(String nodeid) {
        this.nodeid = nodeid;
    }

    public String getCustid() {
        return custid;
    }

    public void setCustid(String custid) {
        this.custid = custid;
    }

    public String getFuncid() {
        return funcid;
    }

    public void setFuncid(String funcid) {
        this.funcid = funcid;
    }

    public String getNetaddr() {
        return netaddr;
    }

    public void setNetaddr(String netaddr) {
        this.netaddr = netaddr;
    }

    public String getOrgid() {
        return orgid;
    }

    public void setOrgid(String orgid) {
        this.orgid = orgid;
    }

    public String getOperway() {
        return operway;
    }

    public void setOperway(String operway) {
        this.operway = operway;
    }

    public String getFundid() {
        return fundid;
    }

    public void setFundid(String fundid) {
        this.fundid = fundid;
    }

    public String getInputtype() {
        return inputtype;
    }

    public void setInputtype(String inputtype) {
        this.inputtype = inputtype;
    }

    public int getFlagstr() {
        return flagstr;
    }

    public void setFlagstr(int flagstr) {
        this.flagstr = flagstr;
    }
}
