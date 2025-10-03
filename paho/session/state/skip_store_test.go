package state

import (
	"bytes"
	"context"
	"testing"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho/session"
	"github.com/stretchr/testify/require"
)

func TestAddToSessionSkipStoreAck(t *testing.T) {
	ss := NewInMemory()
	require.NoError(t, ss.ConAckReceived(&bytes.Buffer{}, &packets.Connect{}, &packets.Connack{}))

	ctx := session.WithAddToSessionOptions(context.Background(), session.AddToSessionOptions{SkipStore: true})
	resp := make(chan packets.ControlPacket, 1)
	pub := &packets.Publish{QoS: 1}

	require.NoError(t, ss.AddToSession(ctx, pub, resp))

	ids, err := ss.clientStore.List()
	require.NoError(t, err)
	require.Len(t, ids, 0)
	require.NotZero(t, pub.PacketID)

	ack := packets.Puback{PacketID: pub.PacketID}
	cp := packets.ControlPacket{
		FixedHeader: packets.FixedHeader{Type: packets.PUBACK},
		Content:     &ack,
	}
	require.NoError(t, ss.PacketReceived(&cp, nil))

	select {
	case r := <-resp:
		require.Equal(t, packets.PUBACK, r.Type)
	default:
		t.Fatal("expected publish response")
	}

	ids, err = ss.clientStore.List()
	require.NoError(t, err)
	require.Len(t, ids, 0)
}

func TestAddToSessionSkipStoreConnectionLost(t *testing.T) {
	ss := NewInMemory()
	require.NoError(t, ss.ConAckReceived(&bytes.Buffer{}, &packets.Connect{}, &packets.Connack{}))

	ctx := session.WithAddToSessionOptions(context.Background(), session.AddToSessionOptions{SkipStore: true})
	resp := make(chan packets.ControlPacket, 1)
	pub := &packets.Publish{QoS: 1}

	require.NoError(t, ss.AddToSession(ctx, pub, resp))

	ids, err := ss.clientStore.List()
	require.NoError(t, err)
	require.Len(t, ids, 0)

	require.NoError(t, ss.ConnectionLost(nil))

	select {
	case r := <-resp:
		require.Equal(t, byte(0), r.Type)
	default:
		t.Fatal("expected publish response after connection loss")
	}

	ids, err = ss.clientStore.List()
	require.NoError(t, err)
	require.Len(t, ids, 0)
	require.Empty(t, ss.clientPackets)
}
