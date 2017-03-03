package com.ebay.oss.griffin.service;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

import com.ebay.oss.griffin.domain.UserSubscription;
import com.ebay.oss.griffin.repo.UserSubscriptionRepo;
import com.ebay.oss.griffin.service.SubscribeServiceImpl;
import com.ebay.oss.griffin.vo.PlatformSubscription;


public class SubscribeServiceImplTest {

    private SubscribeServiceImpl svc;

    String testUser = "lliu13", testId = "testSubscribe";

    @Before
    public void setUp() {
        svc = new SubscribeServiceImpl();
        svc.subscriptionRepo = mock(UserSubscriptionRepo.class);
    }

    @Test
    public void test_subscribe() {


        UserSubscription usi = new UserSubscription();
        usi.setNtaccount(testUser);
        usi.setId(testId);
        usi.setSubscribes(new ArrayList<PlatformSubscription>());

        doNothing().when(svc.subscriptionRepo).upsertUserSubscribe(usi);

        svc.subscribe(usi);
        
        verify(svc.subscriptionRepo).upsertUserSubscribe(usi);
    }
    
    @Test
    public void test_getSubscribe() {
        UserSubscription expect = mock(UserSubscription.class);
        when(svc.subscriptionRepo.getUserSubscribeItem(testUser)).thenReturn(expect);

        UserSubscription actual = svc.getSubscribe(testUser);

        assertNotNull(actual);
        assertSame(expect, actual);
        verify(svc.subscriptionRepo).getUserSubscribeItem(testUser);
    }

}
