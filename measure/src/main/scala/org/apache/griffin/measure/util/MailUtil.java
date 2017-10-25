/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package org.apache.griffin.measure.util;


import org.apache.griffin.measure.config.params.env.EmailParam;

import javax.mail.*;
import javax.mail.internet.*;
import java.util.Properties;

/**
 * Created by xiaoqiu.duan on 2017/9/11.
 */
public class MailUtil {

    public static void SendMail(String name, String UserArr, String Title, String Info, EmailParam emailParam) throws MessagingException {
        Properties prop = new Properties();
        prop.setProperty("mail.transport.protocol", "smtp");
        prop.setProperty("mail.smtp.host", emailParam.host());
        prop.setProperty("mail.smtp.auth", "NTLM");
        prop.setProperty("mail.debug", "true");
        Session session = Session.getInstance(prop);
        Message msg = new MimeMessage(session);
        msg.setFrom(new InternetAddress(emailParam.mail()));
        //msg.setRecipient(Message.RecipientType.TO, new InternetAddress(UserArr));
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
        msg.setSubject(Title);
        Multipart mainPart = new MimeMultipart();
        BodyPart html = new MimeBodyPart();
        html.setContent(Info, "text/html; charset=utf-8");
        mainPart.addBodyPart(html);
        msg.setContent(mainPart);
        msg.addHeader("X-Priority", "3");
        msg.addHeader("X-MSMail-Priority", "Normal");
        msg.addHeader("X-Mailer", "Microsoft Outlook Express 6.00.2900.2869");
        msg.addHeader("X-MimeOLE", "Produced By Microsoft MimeOLE V6.00.2900.2869");
        msg.addHeader("ReturnReceipt", "1");
        Transport trans = session.getTransport();
        trans.connect(emailParam.usr(), emailParam.pwd());
        trans.sendMessage(msg, msg.getAllRecipients());
    }

}
