package autopaho_test

import (
	"context"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	autopaho "github.com/mariotoffia/paho.golang/autopaho"
	"github.com/mariotoffia/paho.golang/internal/testserver"
	"github.com/mariotoffia/paho.golang/packets"
	"github.com/mariotoffia/paho.golang/paho"
	paholog "github.com/mariotoffia/paho.golang/paho/log"
	"github.com/mariotoffia/paho.golang/paho/session"
	"github.com/mariotoffia/paho.golang/paho/session/state"
	memstore "github.com/mariotoffia/paho.golang/paho/store/memory"

	"github.com/stretchr/testify/require"
)

const (
	dummyURL    = "tcp://127.0.0.1:1883"
	shortDelay  = 500 * time.Millisecond
	longerDelay = time.Second
)

type connHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func TestPublishQoS1NoQueueSuccess(t *testing.T) {
	t.Parallel()

	serverURL, _ := url.Parse(dummyURL)
	serverLogger := paholog.NewTestLogger(t, "testServer:")
	logger := paholog.NewTestLogger(t, "test:")

	ts := testserver.New(serverLogger)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tsConnMsg := make(chan connHandle, 1)
	connUp := make(chan struct{}, 1)
	connDown := make(chan struct{}, 1)

	clientStore := memstore.New()
	serverStore := memstore.New()
	sess := state.New(clientStore, serverStore)
	sess.SetDebugLogger(paholog.NewTestLogger(t, "sessionDebug:"))
	sess.SetErrorLogger(paholog.NewTestLogger(t, "sessionError:"))
	t.Cleanup(func() {
		sess.Close()
	})

	cfg := autopaho.ClientConfig{
		ServerUrls:       []*url.URL{serverURL},
		KeepAlive:        60,
		ReconnectBackoff: autopaho.NewConstantBackoff(10 * time.Millisecond),
		ConnectTimeout:   shortDelay,
		AttemptConnection: func(ctx context.Context, _ autopaho.ClientConfig, _ *url.URL) (net.Conn, error) {
			ctx, cancel := context.WithCancel(ctx)
			conn, done, err := ts.Connect(ctx)
			if err == nil {
				tsConnMsg <- connHandle{cancel: cancel, done: done}
			} else {
				cancel()
			}
			return conn, err
		},
		OnConnectionUp: func(*autopaho.ConnectionManager, *paho.Connack) {
			connUp <- struct{}{}
		},
		OnConnectionDown: func() bool {
			connDown <- struct{}{}
			return true
		},
		Debug:                         logger,
		Errors:                        logger,
		PahoDebug:                     logger,
		PahoErrors:                    logger,
		CleanStartOnInitialConnection: false,
		SessionExpiryInterval:         600,
		ClientConfig: paho.ClientConfig{
			ClientID:    "test-client",
			Session:     sess,
			PingHandler: paho.NewDefaultPinger(),
		},
	}

	cm, err := autopaho.NewConnection(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		<-cm.Done()
	})

	select {
	case msg := <-tsConnMsg:
		t.Cleanup(func() {
			msg.cancel()
			<-msg.done
		})
	case <-time.After(shortDelay):
		t.Fatalf("timeout waiting for initial connection request")
	}

	select {
	case <-connUp:
	case <-time.After(shortDelay):
		t.Fatalf("timeout waiting for connection up")
	}

	pubCtx := session.WithAddToSessionOptions(context.Background(), session.AddToSessionOptions{SkipStore: true})
	resp, err := cm.Publish(pubCtx, &paho.Publish{
		QoS:     1,
		Topic:   "test/topic",
		Payload: []byte("payload"),
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	require.NoError(t, cm.Disconnect(ctx))
}

func TestPublishQoS1NoQueueConnectionLost(t *testing.T) {
	t.Parallel()

	serverURL, _ := url.Parse(dummyURL)
	serverLogger := paholog.NewTestLogger(t, "testServer:")
	logger := paholog.NewTestLogger(t, "test:")

	ts := testserver.New(serverLogger)

	payloadCh := make(chan string, 5)
	ts.SetPacketReceivedCallback(func(cp *packets.ControlPacket) error {
		if pub, ok := cp.Content.(*packets.Publish); ok {
			payloadCh <- string(pub.Payload)
		}
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tsConnMsg := make(chan connHandle, 2)
	connUp := make(chan struct{}, 2)
	connDown := make(chan struct{}, 2)

	clientStore := memstore.New()
	serverStore := memstore.New()
	sess := state.New(clientStore, serverStore)
	sess.SetDebugLogger(paholog.NewTestLogger(t, "sessionDebug:"))
	sess.SetErrorLogger(paholog.NewTestLogger(t, "sessionError:"))
	t.Cleanup(func() {
		sess.Close()
	})

	cfg := autopaho.ClientConfig{
		ServerUrls:       []*url.URL{serverURL},
		KeepAlive:        60,
		ReconnectBackoff: autopaho.NewConstantBackoff(10 * time.Millisecond),
		ConnectTimeout:   shortDelay,
		AttemptConnection: func(ctx context.Context, _ autopaho.ClientConfig, _ *url.URL) (net.Conn, error) {
			ctx, cancel := context.WithCancel(ctx)
			conn, done, err := ts.Connect(ctx)
			if err == nil {
				tsConnMsg <- connHandle{cancel: cancel, done: done}
			} else {
				cancel()
			}
			return conn, err
		},
		OnConnectionUp: func(*autopaho.ConnectionManager, *paho.Connack) {
			connUp <- struct{}{}
		},
		OnConnectionDown: func() bool {
			connDown <- struct{}{}
			return true
		},
		Debug:                         logger,
		Errors:                        logger,
		PahoDebug:                     logger,
		PahoErrors:                    logger,
		CleanStartOnInitialConnection: false,
		SessionExpiryInterval:         600,
		ClientConfig: paho.ClientConfig{
			ClientID:    "test-client",
			Session:     sess,
			PingHandler: paho.NewDefaultPinger(),
		},
	}

	cm, err := autopaho.NewConnection(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		<-cm.Done()
	})

	firstConn := <-tsConnMsg
	t.Cleanup(func() {
		firstConn.cancel()
		<-firstConn.done
	})

	select {
	case <-connUp:
	case <-time.After(shortDelay):
		t.Fatalf("timeout waiting for connection up")
	}

	type publishResult struct {
		resp *paho.PublishResponse
		err  error
	}
	resCh := make(chan publishResult, 1)
	go func() {
		pubCtx := session.WithAddToSessionOptions(context.Background(), session.AddToSessionOptions{SkipStore: true})
		resp, err := cm.Publish(pubCtx, &paho.Publish{
			QoS:     1,
			Topic:   "test/topic",
			Payload: []byte(testserver.CloseOnPublishReceived),
		})
		resCh <- publishResult{resp: resp, err: err}
	}()

	select {
	case payload := <-payloadCh:
		require.Equal(t, testserver.CloseOnPublishReceived, payload)
	case <-time.After(shortDelay):
		t.Fatalf("expected publish to reach broker")
	}

	select {
	case res := <-resCh:
		require.Nil(t, res.resp)
		require.Error(t, res.err)
		require.Equal(t, "PUBLISH transmitted but not fully acknowledged at time of shutdown", res.err.Error())
	case <-time.After(longerDelay):
		t.Fatalf("publish did not finish")
	}

	select {
	case <-connDown:
	case <-time.After(longerDelay):
		t.Fatalf("expected connection down notification")
	}

	var reconnection connHandle
	select {
	case reconnection = <-tsConnMsg:
	case <-time.After(longerDelay):
		t.Fatalf("expected reconnection attempt")
	}
	t.Cleanup(func() {
		reconnection.cancel()
		<-reconnection.done
	})

	select {
	case <-connUp:
	case <-time.After(2 * longerDelay):
		t.Fatalf("expected reconnection")
	}

	select {
	case <-payloadCh:
		t.Fatal("publish should not be resent after reconnection")
	case <-time.After(longerDelay):
	}

	ids, listErr := clientStore.List()
	require.NoError(t, listErr)
	require.Len(t, ids, 0, "skip-store publish should not persist entries")

	require.NoError(t, cm.Disconnect(ctx))
}

func TestPublishQoS1NoQueueAckTimeout(t *testing.T) {
	t.Parallel()

	serverURL, _ := url.Parse(dummyURL)
	serverLogger := paholog.NewTestLogger(t, "testServer:")
	logger := paholog.NewTestLogger(t, "test:")

	ts := testserver.New(serverLogger)

	const ackTimeout = 150 * time.Millisecond
	publishReceived := make(chan string, 1)
	ts.SetPacketReceivedCallback(func(cp *packets.ControlPacket) error {
		pub, ok := cp.Content.(*packets.Publish)
		if !ok {
			return nil
		}
		if pub.QoS == 1 {
			publishReceived <- string(pub.Payload)
			time.Sleep(2 * ackTimeout)
		}
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tsConnMsg := make(chan connHandle, 1)
	connUp := make(chan struct{}, 1)
	connDown := make(chan struct{}, 1)

	clientStore := memstore.New()
	serverStore := memstore.New()
	sess := state.New(clientStore, serverStore)
	sess.SetDebugLogger(paholog.NewTestLogger(t, "sessionDebug:"))
	sess.SetErrorLogger(paholog.NewTestLogger(t, "sessionError:"))
	t.Cleanup(func() {
		sess.Close()
	})

	cfg := autopaho.ClientConfig{
		ServerUrls:       []*url.URL{serverURL},
		KeepAlive:        60,
		ReconnectBackoff: autopaho.NewConstantBackoff(10 * time.Millisecond),
		ConnectTimeout:   shortDelay,
		AttemptConnection: func(ctx context.Context, _ autopaho.ClientConfig, _ *url.URL) (net.Conn, error) {
			ctx, cancel := context.WithCancel(ctx)
			conn, done, err := ts.Connect(ctx)
			if err == nil {
				tsConnMsg <- connHandle{cancel: cancel, done: done}
			} else {
				cancel()
			}
			return conn, err
		},
		OnConnectionUp: func(*autopaho.ConnectionManager, *paho.Connack) {
			connUp <- struct{}{}
		},
		OnConnectionDown: func() bool {
			connDown <- struct{}{}
			return true
		},
		Debug:                         logger,
		Errors:                        logger,
		PahoDebug:                     logger,
		PahoErrors:                    logger,
		CleanStartOnInitialConnection: false,
		SessionExpiryInterval:         600,
		ClientConfig: paho.ClientConfig{
			ClientID:      "test-client",
			Session:       sess,
			PingHandler:   paho.NewDefaultPinger(),
			PacketTimeout: ackTimeout,
		},
	}

	cm, err := autopaho.NewConnection(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		<-cm.Done()
	})

	select {
	case msg := <-tsConnMsg:
		t.Cleanup(func() {
			msg.cancel()
			<-msg.done
		})
	case <-time.After(shortDelay):
		t.Fatalf("timeout waiting for initial connection request")
	}

	select {
	case <-connUp:
	case <-time.After(shortDelay):
		t.Fatalf("timeout waiting for connection up")
	}

	pubCtx := session.WithAddToSessionOptions(context.Background(), session.AddToSessionOptions{SkipStore: true})
	resultCh := make(chan struct{})
	var resp *paho.PublishResponse
	var publishErr error
	go func() {
		resp, publishErr = cm.Publish(pubCtx, &paho.Publish{
			QoS:     1,
			Topic:   "test/topic",
			Payload: []byte("qos1-timeout"),
		})
		close(resultCh)
	}()

	select {
	case payload := <-publishReceived:
		require.Equal(t, "qos1-timeout", payload)
	case <-time.After(shortDelay):
		t.Fatalf("expected publish to reach broker")
	}

	select {
	case <-resultCh:
	case <-time.After(2 * longerDelay):
		t.Fatalf("publish did not complete")
	}
	require.Nil(t, resp)
	require.Error(t, publishErr)
	require.ErrorIs(t, publishErr, context.DeadlineExceeded)

	select {
	case <-connDown:
		t.Fatal("connection should remain up when ack times out")
	case <-time.After(shortDelay):
	}

	require.NoError(t, cm.Disconnect(ctx))
}

func TestPublishQoS2NoQueueAckTimeout(t *testing.T) {
	t.Parallel()

	serverURL, _ := url.Parse(dummyURL)
	serverLogger := paholog.NewTestLogger(t, "testServer:")
	logger := paholog.NewTestLogger(t, "test:")

	ts := testserver.New(serverLogger)

	const ackTimeout = 150 * time.Millisecond
	publishReceived := make(chan string, 1)
	ts.SetPacketReceivedCallback(func(cp *packets.ControlPacket) error {
		switch cp.Type {
		case packets.PUBLISH:
			pub := cp.Content.(*packets.Publish)
			if pub.QoS == 2 {
				publishReceived <- string(pub.Payload)
				time.Sleep(2 * ackTimeout)
			}
		case packets.PUBREL:
			// allow PUBREL handling without delay in this test
		}
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tsConnMsg := make(chan connHandle, 1)
	connUp := make(chan struct{}, 1)
	connDown := make(chan struct{}, 1)

	clientStore := memstore.New()
	serverStore := memstore.New()
	sess := state.New(clientStore, serverStore)
	sess.SetDebugLogger(paholog.NewTestLogger(t, "sessionDebug:"))
	sess.SetErrorLogger(paholog.NewTestLogger(t, "sessionError:"))
	t.Cleanup(func() {
		sess.Close()
	})

	cfg := autopaho.ClientConfig{
		ServerUrls:       []*url.URL{serverURL},
		KeepAlive:        60,
		ReconnectBackoff: autopaho.NewConstantBackoff(10 * time.Millisecond),
		ConnectTimeout:   shortDelay,
		AttemptConnection: func(ctx context.Context, _ autopaho.ClientConfig, _ *url.URL) (net.Conn, error) {
			ctx, cancel := context.WithCancel(ctx)
			conn, done, err := ts.Connect(ctx)
			if err == nil {
				tsConnMsg <- connHandle{cancel: cancel, done: done}
			} else {
				cancel()
			}
			return conn, err
		},
		OnConnectionUp: func(*autopaho.ConnectionManager, *paho.Connack) {
			connUp <- struct{}{}
		},
		OnConnectionDown: func() bool {
			connDown <- struct{}{}
			return true
		},
		Debug:                         logger,
		Errors:                        logger,
		PahoDebug:                     logger,
		PahoErrors:                    logger,
		CleanStartOnInitialConnection: false,
		SessionExpiryInterval:         600,
		ClientConfig: paho.ClientConfig{
			ClientID:      "test-client",
			Session:       sess,
			PingHandler:   paho.NewDefaultPinger(),
			PacketTimeout: ackTimeout,
		},
	}

	cm, err := autopaho.NewConnection(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		<-cm.Done()
	})

	select {
	case msg := <-tsConnMsg:
		t.Cleanup(func() {
			msg.cancel()
			<-msg.done
		})
	case <-time.After(shortDelay):
		t.Fatalf("timeout waiting for initial connection request")
	}

	select {
	case <-connUp:
	case <-time.After(shortDelay):
		t.Fatalf("timeout waiting for connection up")
	}

	pubCtx := session.WithAddToSessionOptions(context.Background(), session.AddToSessionOptions{SkipStore: true})
	resultCh := make(chan struct{})
	var resp *paho.PublishResponse
	var publishErr error
	go func() {
		resp, publishErr = cm.Publish(pubCtx, &paho.Publish{
			QoS:     2,
			Topic:   "test/topic",
			Payload: []byte("qos2-timeout-publish"),
		})
		close(resultCh)
	}()

	select {
	case payload := <-publishReceived:
		require.Equal(t, "qos2-timeout-publish", payload)
	case <-time.After(shortDelay):
		t.Fatalf("expected publish to reach broker")
	}

	select {
	case <-resultCh:
	case <-time.After(2 * longerDelay):
		t.Fatalf("publish did not complete")
	}
	require.Nil(t, resp)
	require.Error(t, publishErr)
	require.ErrorIs(t, publishErr, context.DeadlineExceeded)

	select {
	case <-connDown:
		t.Fatal("connection should remain up when ack times out")
	case <-time.After(shortDelay):
	}

	require.NoError(t, cm.Disconnect(ctx))
}

func TestPublishQoS2NoQueueAckTimeoutAfterPubrel(t *testing.T) {
	t.Parallel()

	serverURL, _ := url.Parse(dummyURL)
	serverLogger := paholog.NewTestLogger(t, "testServer:")
	logger := paholog.NewTestLogger(t, "test:")

	ts := testserver.New(serverLogger)

	const ackTimeout = 150 * time.Millisecond
	publishReceived := make(chan string, 1)
	pubrelObserved := make(chan struct{}, 1)
	var once sync.Once
	ts.SetPacketReceivedCallback(func(cp *packets.ControlPacket) error {
		switch cp.Type {
		case packets.PUBLISH:
			pub := cp.Content.(*packets.Publish)
			if pub.QoS == 2 {
				publishReceived <- string(pub.Payload)
			}
		case packets.PUBREL:
			once.Do(func() {
				pubrelObserved <- struct{}{}
				time.Sleep(2 * ackTimeout)
			})
		}
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tsConnMsg := make(chan connHandle, 1)
	connUp := make(chan struct{}, 1)
	connDown := make(chan struct{}, 1)

	clientStore := memstore.New()
	serverStore := memstore.New()
	sess := state.New(clientStore, serverStore)
	sess.SetDebugLogger(paholog.NewTestLogger(t, "sessionDebug:"))
	sess.SetErrorLogger(paholog.NewTestLogger(t, "sessionError:"))
	t.Cleanup(func() {
		sess.Close()
	})

	cfg := autopaho.ClientConfig{
		ServerUrls:       []*url.URL{serverURL},
		KeepAlive:        60,
		ReconnectBackoff: autopaho.NewConstantBackoff(10 * time.Millisecond),
		ConnectTimeout:   shortDelay,
		AttemptConnection: func(ctx context.Context, _ autopaho.ClientConfig, _ *url.URL) (net.Conn, error) {
			ctx, cancel := context.WithCancel(ctx)
			conn, done, err := ts.Connect(ctx)
			if err == nil {
				tsConnMsg <- connHandle{cancel: cancel, done: done}
			} else {
				cancel()
			}
			return conn, err
		},
		OnConnectionUp: func(*autopaho.ConnectionManager, *paho.Connack) {
			connUp <- struct{}{}
		},
		OnConnectionDown: func() bool {
			connDown <- struct{}{}
			return true
		},
		Debug:                         logger,
		Errors:                        logger,
		PahoDebug:                     logger,
		PahoErrors:                    logger,
		CleanStartOnInitialConnection: false,
		SessionExpiryInterval:         600,
		ClientConfig: paho.ClientConfig{
			ClientID:      "test-client",
			Session:       sess,
			PingHandler:   paho.NewDefaultPinger(),
			PacketTimeout: ackTimeout,
		},
	}

	cm, err := autopaho.NewConnection(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		<-cm.Done()
	})

	select {
	case msg := <-tsConnMsg:
		t.Cleanup(func() {
			msg.cancel()
			<-msg.done
		})
	case <-time.After(shortDelay):
		t.Fatalf("timeout waiting for initial connection request")
	}

	select {
	case <-connUp:
	case <-time.After(shortDelay):
		t.Fatalf("timeout waiting for connection up")
	}

	pubCtx := session.WithAddToSessionOptions(context.Background(), session.AddToSessionOptions{SkipStore: true})
	resultCh := make(chan struct{})
	var resp *paho.PublishResponse
	var publishErr error
	go func() {
		resp, publishErr = cm.Publish(pubCtx, &paho.Publish{
			QoS:     2,
			Topic:   "test/topic",
			Payload: []byte("qos2-timeout-pubrel"),
		})
		close(resultCh)
	}()

	select {
	case payload := <-publishReceived:
		require.Equal(t, "qos2-timeout-pubrel", payload)
	case <-time.After(shortDelay):
		t.Fatalf("expected publish to reach broker")
	}

	select {
	case <-pubrelObserved:
	case <-time.After(shortDelay):
		t.Fatalf("expected PUBREL to be observed")
	}

	select {
	case <-resultCh:
	case <-time.After(2 * longerDelay):
		t.Fatalf("publish did not complete")
	}
	require.Nil(t, resp)
	require.Error(t, publishErr)
	require.ErrorIs(t, publishErr, context.DeadlineExceeded)

	select {
	case <-connDown:
		t.Fatal("connection should remain up when ack times out")
	case <-time.After(shortDelay):
	}

	require.NoError(t, cm.Disconnect(ctx))
}

func TestPublishQoS2NoQueueConnectionLost(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		payload string
	}{
		{name: "CloseOnPublish", payload: testserver.CloseOnPublishReceived},
		{name: "CloseOnPubRel", payload: testserver.CloseOnPubRelReceived},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			serverURL, _ := url.Parse(dummyURL)
			serverLogger := paholog.NewTestLogger(t, "testServer:")
			logger := paholog.NewTestLogger(t, "test:")

			ts := testserver.New(serverLogger)

			payloadCh := make(chan string, 5)
			ts.SetPacketReceivedCallback(func(cp *packets.ControlPacket) error {
				if pub, ok := cp.Content.(*packets.Publish); ok {
					payloadCh <- string(pub.Payload)
				}
				return nil
			})

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			tsConnMsg := make(chan connHandle, 2)
			connUp := make(chan struct{}, 2)
			connDown := make(chan struct{}, 2)

			clientStore := memstore.New()
			serverStore := memstore.New()
			sess := state.New(clientStore, serverStore)
			sess.SetDebugLogger(paholog.NewTestLogger(t, "sessionDebug:"))
			sess.SetErrorLogger(paholog.NewTestLogger(t, "sessionError:"))
			t.Cleanup(func() {
				sess.Close()
			})

			cfg := autopaho.ClientConfig{
				ServerUrls:       []*url.URL{serverURL},
				KeepAlive:        60,
				ReconnectBackoff: autopaho.NewConstantBackoff(10 * time.Millisecond),
				ConnectTimeout:   shortDelay,
				AttemptConnection: func(ctx context.Context, _ autopaho.ClientConfig, _ *url.URL) (net.Conn, error) {
					ctx, cancel := context.WithCancel(ctx)
					conn, done, err := ts.Connect(ctx)
					if err == nil {
						tsConnMsg <- connHandle{cancel: cancel, done: done}
					} else {
						cancel()
					}
					return conn, err
				},
				OnConnectionUp: func(*autopaho.ConnectionManager, *paho.Connack) {
					connUp <- struct{}{}
				},
				OnConnectionDown: func() bool {
					connDown <- struct{}{}
					return true
				},
				Debug:                         logger,
				Errors:                        logger,
				PahoDebug:                     logger,
				PahoErrors:                    logger,
				CleanStartOnInitialConnection: false,
				SessionExpiryInterval:         600,
				ClientConfig: paho.ClientConfig{
					ClientID:    "test-client",
					Session:     sess,
					PingHandler: paho.NewDefaultPinger(),
				},
			}

			cm, err := autopaho.NewConnection(ctx, cfg)
			require.NoError(t, err)
			t.Cleanup(func() {
				<-cm.Done()
			})

			firstConn := <-tsConnMsg
			t.Cleanup(func() {
				firstConn.cancel()
				<-firstConn.done
			})

			select {
			case <-connUp:
			case <-time.After(shortDelay):
				t.Fatalf("timeout waiting for connection up")
			}

			type publishResult struct {
				resp *paho.PublishResponse
				err  error
			}
			resCh := make(chan publishResult, 1)
			go func() {
				pubCtx := session.WithAddToSessionOptions(context.Background(), session.AddToSessionOptions{SkipStore: true})
				resp, err := cm.Publish(pubCtx, &paho.Publish{
					QoS:     2,
					Topic:   "test/topic",
					Payload: []byte(tc.payload),
				})
				resCh <- publishResult{resp: resp, err: err}
			}()

			select {
			case payload := <-payloadCh:
				require.Equal(t, tc.payload, payload)
			case <-time.After(shortDelay):
				t.Fatalf("expected publish to reach broker")
			}

			select {
			case res := <-resCh:
				require.Nil(t, res.resp)
				require.Error(t, res.err)
				require.Equal(t, "PUBLISH transmitted but not fully acknowledged at time of shutdown", res.err.Error())
			case <-time.After(2 * longerDelay):
				t.Fatalf("publish did not finish")
			}

			select {
			case <-connDown:
			case <-time.After(2 * longerDelay):
				t.Fatalf("expected connection down notification")
			}

			var reconnection connHandle
			select {
			case reconnection = <-tsConnMsg:
			case <-time.After(2 * longerDelay):
				t.Fatalf("expected reconnection attempt")
			}
			t.Cleanup(func() {
				reconnection.cancel()
				<-reconnection.done
			})

			select {
			case <-connUp:
			case <-time.After(2 * longerDelay):
				t.Fatalf("expected reconnection")
			}

			select {
			case <-payloadCh:
				t.Fatal("publish should not be resent after reconnection")
			case <-time.After(longerDelay):
			}

			ids, listErr := clientStore.List()
			require.NoError(t, listErr)
			require.Len(t, ids, 0, "skip-store publish should not persist entries")

			require.NoError(t, cm.Disconnect(ctx))
		})
	}
}
