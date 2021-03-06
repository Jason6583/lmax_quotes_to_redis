package com.lmax.api;

import java.io.Writer;
import java.net.URL;

import com.lmax.api.account.AccountDetails;
import com.lmax.api.account.AccountStateEventListener;
import com.lmax.api.account.AccountStateRequest;
import com.lmax.api.heartbeat.HeartbeatCallback;
import com.lmax.api.heartbeat.HeartbeatEventListener;
import com.lmax.api.heartbeat.HeartbeatRequest;
import com.lmax.api.marketdata.HistoricMarketDataRequest;
import com.lmax.api.order.AmendStopsRequest;
import com.lmax.api.order.CancelOrderRequest;
import com.lmax.api.order.ClosingOrderSpecification;
import com.lmax.api.order.ExecutionEventListener;
import com.lmax.api.order.LimitOrderSpecification;
import com.lmax.api.order.MarketOrderSpecification;
import com.lmax.api.order.OrderCallback;
import com.lmax.api.order.OrderEventListener;
import com.lmax.api.orderbook.HistoricMarketDataEventListener;
import com.lmax.api.orderbook.OrderBookEventListener;
import com.lmax.api.orderbook.OrderBookStatusEventListener;
import com.lmax.api.orderbook.SearchInstrumentCallback;
import com.lmax.api.orderbook.SearchInstrumentRequest;
import com.lmax.api.position.PositionEventListener;
import com.lmax.api.reject.InstructionRejectedEventListener;

public class ProtectedSession1 implements Session
{
    public static final String SESSION_DISCONNECTED_TOKEN = "Session Disconnected";
    private final Session session;

    public ProtectedSession1(Session session)
    {
        this.session = session;
    }

    public void start()
    {
        session.start();
    }

    public void stop()
    {
        session.stop();
    }

    public boolean isRunning()
    {
        return session.isRunning();
    }

    public void logout(Callback callback)
    {
        session.logout(callback);
    }

    public void placeMarketOrder(MarketOrderSpecification marketOrderSpecification, OrderCallback orderResponseCallback)
    {
        if (session.isRunning())
        {
            session.placeMarketOrder(marketOrderSpecification, orderResponseCallback);
        }
        else
        {
            orderResponseCallback.onFailure(new FailureResponse(true, SESSION_DISCONNECTED_TOKEN));
        }
    }

    public void placeLimitOrder(LimitOrderSpecification limitOrderSpecification, OrderCallback orderResponseCallback)
    {
        if (session.isRunning())
        {
            session.placeLimitOrder(limitOrderSpecification, orderResponseCallback);
        }
        else
        {
            orderResponseCallback.onFailure(new FailureResponse(true, SESSION_DISCONNECTED_TOKEN));
        }
    }

    public void cancelOrder(CancelOrderRequest cancelOrderRequest, OrderCallback orderResponseCallback)
    {
        if (session.isRunning())
        {
            session.cancelOrder(cancelOrderRequest, orderResponseCallback);
        }
        else
        {
            orderResponseCallback.onFailure(new FailureResponse(true, SESSION_DISCONNECTED_TOKEN));
        }
    }

    public void placeClosingOrder(ClosingOrderSpecification closingOrderSpecification, OrderCallback orderResponseCallback)
    {
        if (session.isRunning())
        {
            session.placeClosingOrder(closingOrderSpecification, orderResponseCallback);
        }
        else
        {
            orderResponseCallback.onFailure(new FailureResponse(true, SESSION_DISCONNECTED_TOKEN));
        }
    }

    public void amendStops(AmendStopsRequest amendStopLossProfitRequest, OrderCallback orderResponseCallback)
    {
        if (session.isRunning())
        {
            session.amendStops(amendStopLossProfitRequest, orderResponseCallback);
        }
        else
        {
            orderResponseCallback.onFailure(new FailureResponse(true, SESSION_DISCONNECTED_TOKEN));
        }
    }

    public void subscribe(SubscriptionRequest subscriptionRequest, Callback callback)
    {
        session.subscribe(subscriptionRequest, callback);
    }

    public void registerAccountStateEventListener(AccountStateEventListener accountStateEventListener)
    {
        session.registerAccountStateEventListener(accountStateEventListener);
    }

    public void registerExecutionEventListener(ExecutionEventListener executionListener)
    {
        session.registerExecutionEventListener(executionListener);
    }

    public void registerHeartbeatListener(HeartbeatEventListener heartbeatEventListener)
    {
        session.registerHeartbeatListener(heartbeatEventListener);
    }

    public void registerHistoricMarketDataEventListener(HistoricMarketDataEventListener historicMarketDataEventListener)
    {
        session.registerHistoricMarketDataEventListener(historicMarketDataEventListener);
    }

    public void registerInstructionRejectedEventListener(InstructionRejectedEventListener instructionRejectedEventListener)
    {
        session.registerInstructionRejectedEventListener(instructionRejectedEventListener);
    }

    public void registerOrderBookEventListener(OrderBookEventListener orderBookEventListener)
    {
        session.registerOrderBookEventListener(orderBookEventListener);
    }

    public void registerOrderBookStatusEventListener(OrderBookStatusEventListener eventListener)
    {
        session.registerOrderBookStatusEventListener(eventListener);
    }

    public void registerOrderEventListener(OrderEventListener orderEventListener)
    {
        session.registerOrderEventListener(orderEventListener);
    }

    public void registerPositionEventListener(PositionEventListener positionEventListener)
    {
        session.registerPositionEventListener(positionEventListener);
    }

    public void registerStreamFailureListener(StreamFailureListener aStreamFailureListener)
    {
        session.registerStreamFailureListener(aStreamFailureListener);
    }

    public void registerSessionDisconnectedListener(SessionDisconnectedListener sessionDisconnectedListener)
    {
        session.registerSessionDisconnectedListener(sessionDisconnectedListener);
    }

    public AccountDetails getAccountDetails()
    {
        return session.getAccountDetails();
    }

    public void requestAccountState(AccountStateRequest accountStateRequest, Callback callback)
    {
        session.requestAccountState(accountStateRequest, callback);
    }

    public void requestHistoricMarketData(HistoricMarketDataRequest historicMarketDataRequest, Callback callback)
    {
        session.requestHistoricMarketData(historicMarketDataRequest, callback);
    }

    public void requestHeartbeat(HeartbeatRequest heartbeatRequest, HeartbeatCallback heartBeatCallback)
    {
        session.requestHeartbeat(heartbeatRequest, heartBeatCallback);
    }

    public void searchInstruments(SearchInstrumentRequest searchRequest, SearchInstrumentCallback searchCallback)
    {
        session.searchInstruments(searchRequest, searchCallback);
    }

    public void openUrl(URL url, UrlCallback urlCallback)
    {
        session.openUrl(url, urlCallback);
    }

    public void setEventStreamDebug(Writer writer)
    {
        session.setEventStreamDebug(writer);
    }
}
