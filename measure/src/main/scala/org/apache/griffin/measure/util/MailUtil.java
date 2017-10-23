package org.apache.griffin.measure.util;


import javax.mail.*;
import javax.mail.internet.*;
import java.util.Properties;

/**
 * Created by xiaoqiu.duan on 2017/9/11.
 */
public class MailUtil {

    public static void SendMail(String name,String UserArr,String Title,String Info) throws MessagingException {
        Properties prop = new Properties();
        prop.setProperty("mail.transport.protocol", "smtp"); //协议
        prop.setProperty("mail.smtp.host", "smtp.163.com"); //主机名
        prop.setProperty("mail.smtp.auth", "NTLM"); //是否开启权限控制
        prop.setProperty("mail.debug", "true"); //返回发送的cmd源码
        Session session = Session.getInstance(prop);
        Message msg = new MimeMessage(session);
        msg.setFrom(new InternetAddress("xiaoqiu2017wy@163.com")); //自己的email
        //msg.setRecipient(Message.RecipientType.TO, new InternetAddress(UserArr)); // 要发送的email，可以设置数组
        String[] arr=null;
        if (UserArr.indexOf(",")==-1) {
            arr=new String[]{UserArr};
        }
        else {
            arr=UserArr.split(",");
        }
        Address[] tos = new InternetAddress[arr.length];
        for (int i=0; i<arr.length; i++){
            tos[i] = new InternetAddress(arr[i]);
        }
        msg.setRecipients(Message.RecipientType.TO,tos);
        msg.setSubject(Title);//邮件标题
        // MiniMultipart类是一个容器类，包含MimeBodyPart类型的对象
        Multipart mainPart = new MimeMultipart();
        // 创建一个包含HTML内容的MimeBodyPart
        BodyPart html = new MimeBodyPart();
        // 设置HTML内容
        html.setContent(Info, "text/html; charset=utf-8");
        mainPart.addBodyPart(html);
        // 将MiniMultipart对象设置为邮件内容
        msg.setContent(mainPart);
        //msg.setText(Info);//邮件正文
        //不被当作垃圾邮件的关键代码--Begin ，如果不加这些代码，发送的邮件会自动进入对方的垃圾邮件列表
        msg.addHeader("X-Priority", "3");
        msg.addHeader("X-MSMail-Priority", "Normal");
        msg.addHeader("X-Mailer", "Microsoft Outlook Express 6.00.2900.2869"); //本文以outlook名义发送邮件，不会被当作垃圾邮件
        msg.addHeader("X-MimeOLE", "Produced By Microsoft MimeOLE V6.00.2900.2869");
        msg.addHeader("ReturnReceipt", "1");
        //不被当作垃圾邮件的关键代码--end
        Transport trans = session.getTransport();
        trans.connect("xiaoqiu2017wy@163.com", "xiaoqiu2017"); // 邮件的账号密码
        trans.sendMessage(msg, msg.getAllRecipients());
    }

}
