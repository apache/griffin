package com.ebay.oss.griffin.service;

import static org.junit.Assert.*;

import java.util.Date;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ebay.oss.griffin.service.NotificationService;
import com.ebay.oss.griffin.vo.NotificationRecord;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:context.xml"})
public class NotificationServiceImplTest {

    @Autowired
    private NotificationService notificationService;

    private List<NotificationRecord> testGetAll() {
        return notificationService.getAll();
    }

    private void testInsert(int n) {
        System.out.println("===== Insert " + n + " Notification Records =====");
        for (int i = 1; i <= n; i++) {
            NotificationRecord rcd = new NotificationRecord(new Date().getTime(), "lliu13", "Operation" + i, "Target" + i, "NotifyRecord" + i);
            notificationService.insert(rcd);
        }
        System.out.println();
    }

    private NotificationRecord testGet(int id, boolean expectNull) {
        NotificationRecord fnr = notificationService.get(id);
        if (expectNull) {
            assertNull(fnr);
        } else {
            assertNotNull(fnr);
            System.out.println("get notification record: " + fnr.getName() + ": " + fnr.getTarget() + " " + fnr.getOperation());
        }
        return fnr;
    }

    private void testGetTop(int n) {
        System.out.println("===== Get Top " + n + " Notification Record =====");
        List<NotificationRecord> fnrList = notificationService.getTop(n);
        assertEquals(fnrList.size(), n);
        for (NotificationRecord nr : fnrList) {
            System.out.println(nr.getName() + ": " + nr.getTarget() + " " + nr.getOperation());
        }
        System.out.println();
    }

    private void testDelete(NotificationRecord nfr) {
        notificationService.delete(nfr);
    }
    private void testDelete(int id) {
        notificationService.delete(id);
    }

    @Test
    public void TestNotificationService() {

        //getAll
        System.out.println("===== All Notification Records 0 =====");
        List<NotificationRecord> nrList = testGetAll();
        int sz = nrList.size();
        System.out.println("current notification records list size: " + sz);
        System.out.println();

        //insert
        int nInsert = 7;
        testInsert(nInsert);

        //getAll
        System.out.println("===== All Notification Records 1 =====");
        int sz1 = nrList.size();
        assertEquals(sz1 - sz, nInsert);
        System.out.println("current notification records list size: " + sz1);
        System.out.println();

        //get
        System.out.println("===== Get Notification Record =====");
        NotificationRecord fnr = testGet(3, false);
        testGet(9009, true);

        //getTop
        testGetTop(5);

        //delete
        System.out.println("===== delete 2 Notification Records =====");
        testDelete(fnr);
        testDelete(6);
        NotificationRecord nrcd = new NotificationRecord(new Date().getTime(), "lliu13", "Operation", "Target", "NotifyRecord");
        testDelete(nrcd);
        System.out.println();

        //getAll
        System.out.println("===== All Notification Records 2 =====");
        int sz2 = nrList.size();
        assertEquals(sz1 - sz2, 2);
        System.out.println("current notification records list size: " + sz2);
        System.out.println();
    }
}
