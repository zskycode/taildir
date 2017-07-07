package org.apache.flume.source.taildir.windows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.WinBase;
import com.sun.jna.platform.win32.WinNT.HANDLE;

import java.io.File;
import java.nio.file.Files;
/**
 * Created by zhuzhigang on 2017/7/6.
 */
public class WinFileUtil {

    public  static WinFileUtil getWinFile(){
        return  new WinFileUtil();
    }
    private static Logger logger = LoggerFactory.getLogger(WinFileUtil.class);

    public static String getFileId(String filepath) {

        final int FILE_SHARE_READ = (0x00000001);
        final int OPEN_EXISTING = (3);
        final int GENERIC_READ = (0x80000000);
        final int FILE_ATTRIBUTE_ARCHIVE = (0x20);

        WinBase.SECURITY_ATTRIBUTES attr = null;
        org.apache.flume.source.taildir.windows.Kernel32.BY_HANDLE_FILE_INFORMATION lpFileInformation = new org.apache.flume.source.taildir.windows.Kernel32.BY_HANDLE_FILE_INFORMATION();
        HANDLE hFile = null;

        hFile = Kernel32.INSTANCE.CreateFile(filepath, 0,
                FILE_SHARE_READ, attr, OPEN_EXISTING, FILE_ATTRIBUTE_ARCHIVE,
                null);
        String ret = "0";
        if (Kernel32.INSTANCE.GetLastError() == 0) {

            org.apache.flume.source.taildir.windows.Kernel32.INSTANCE
                    .GetFileInformationByHandle(hFile, lpFileInformation);

            ret = lpFileInformation.dwVolumeSerialNumber.toString()
                    + lpFileInformation.nFileIndexLow.toString();

            Kernel32.INSTANCE.CloseHandle(hFile);

            if (Kernel32.INSTANCE.GetLastError() == 0) {
                logger.debug("inode:" + ret);
                return ret;
            } else {
                logger.error("关闭文件发生错误:{}", filepath);
                throw new RuntimeException("关闭文件发生错误:" + filepath);
            }
        } else {
            if (hFile != null) {
                Kernel32.INSTANCE.CloseHandle(hFile);
            }
            logger.error("打开文件发生错误:{}", filepath);
            throw new RuntimeException("打开文件发生错误:" + filepath);
        }

    }

    public static void main(String[] args) throws Exception {
        File f=new File("fileInfo.log");
        System.out.println(f.toPath());
        System.out.println(Long.valueOf("1232188718728378"));
        System.out.println("file ino: "+Files.getAttribute(f.toPath(), "unix:ino"));
    }
}
