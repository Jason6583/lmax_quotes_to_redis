package com.lmax.api;

import com.lmax.api.account.LoginCallback;
import com.lmax.api.account.LoginRequest;
import com.lmax.api.heartbeat.HeartbeatCallback;
import com.lmax.api.heartbeat.HeartbeatEventListener;
import com.lmax.api.heartbeat.HeartbeatRequest;
import com.lmax.api.heartbeat.HeartbeatSubscriptionRequest;
import com.lmax.api.orderbook.OrderBookEvent;
import com.lmax.api.orderbook.OrderBookEventListener;
import com.lmax.api.orderbook.OrderBookSubscriptionRequest;
import com.lmax.api.orderbook.PricePoint;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import redis.clients.jedis.Jedis;

public class MarketDataSubscriber implements LoginCallback, HeartbeatEventListener, OrderBookEventListener, Runnable {

    private Session session;
    private static String instrumentIDs[];
    private static Jedis jedis;
    private long counter = 0;

    @Override
    public void onLoginSuccess(final Session session) {
        
        session.registerOrderBookEventListener(this);

        try {
            for (int i = 0; i < instrumentIDs.length; i++) {
                if (instrumentIDs[i] != null && instrumentIDs[i].length() > 0) {
                    subscribeToInstrument(session, Long.parseLong(instrumentIDs[i]));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        this.session = session;
        this.session.registerHeartbeatListener(this);
        
        session.subscribe(new HeartbeatSubscriptionRequest(), new Callback()
        {
            public void onSuccess()
            {
            }

            @Override
            public void onFailure(final FailureResponse failureResponse)
            {
                throw new RuntimeException("Failed");
            }
        });
        
        new Thread(this).start();

        session.start();
    }

    private void subscribeToInstrument(final Session session, final long instrumentId) {
        
        session.subscribe(new OrderBookSubscriptionRequest(instrumentId), new Callback() {
            public void onSuccess() {
                System.out.printf("Subscribed to instrument %d.%n", instrumentId);
            }

            public void onFailure(final FailureResponse failureResponse) {
                System.err.printf("Failed to subscribe to instrument %d: %s%n", instrumentId, failureResponse);
            }
        });        
    }

    @Override
    @SuppressWarnings({"ThrowableResultOfMethodCallIgnored"})
    public void onLoginFailure(final FailureResponse failureResponse) {
        throw new RuntimeException("Unable to login: " + failureResponse.getDescription(), failureResponse.getException());
    }

    @Override
    public void notify(final OrderBookEvent orderBookEvent) {
//        System.out.println(orderBookEvent);
        String val = getRoundTripTime(orderBookEvent.getTimeStamp()) + "," + orderBookEvent.getInstrumentId() + ","
                + orderBookEvent.getValuationBidPrice() + "," + orderBookEvent.getValuationAskPrice();
        System.out.println(val);
        //jedis.set("lmax_quote", val);
        try {
            jedis.publish("lmax_quote", val);
        } catch (Exception e) {
            System.err.printf("Failed to redis publish to instrument %d: %s%n", orderBookEvent.getInstrumentId(), e.getMessage());
        }

    }

    private String getRoundTripTime(long time) {
        String roundTripTime = "";
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            roundTripTime = "" + dateFormat.format(new Date(time));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return roundTripTime;
    }

    private FixedPointNumber getBestPrice(List<PricePoint> prices) {
        return prices.size() != 0 ? prices.get(0).getPrice() : FixedPointNumber.ZERO;
    }

    public static void main(String[] args) {
        if (args.length != 7) {
            System.out.println("Usage " + MarketDataSubscriber.class.getName() + " <url> <username> <password> [CFD_DEMO|CFD_LIVE] [instrumentIds...] <redishost> <redisport>");
            System.exit(-1);
        }

        try {
            String url = args[0];
            String username = args[1];
            String password = args[2];
            LoginRequest.ProductType productType = LoginRequest.ProductType.valueOf(args[3].toUpperCase());

            instrumentIDs = args[4].split(",");

            jedis = new Jedis(args[5], Integer.parseInt(args[6]));

            LmaxApi lmaxApi = new LmaxApi(url);
            MarketDataSubscriber marketDataRequester = new MarketDataSubscriber();

            lmaxApi.login(new LoginRequest(username, password, productType), marketDataRequester);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void notify(long accountId, String token) {
        System.out.printf("Received heartbeat: %d, %s%n", accountId, token);
    }

    private void requestHeartbeat()
    {
        this.session.requestHeartbeat(new HeartbeatRequest("heartbeat-" + counter++), new HeartbeatCallback()
        {
            @Override
            public void onSuccess(String token)
            {
                System.out.println("Requested heartbeat: " + token);
            }
            
            @Override
            public void onFailure(FailureResponse failureResponse)
            {
                throw new RuntimeException("Failed");
            }
        });
    }
    
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                Thread.sleep(60000);

                requestHeartbeat();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}