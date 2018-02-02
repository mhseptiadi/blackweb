package gocbcore

import (
	"encoding/binary"
)

// Observe retrieves the current CAS and persistence state for a document.
func (agent *Agent) Observe(key []byte, replicaIdx int, cb ObserveCallback) (PendingOp, error) {
	// TODO(mnunberg): Use bktType when implemented
	if agent.numVbuckets == 0 {
		return nil, ErrNotSupported
	}

	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			cb(0, 0, err)
			return
		}

		if len(resp.Value) < 4 {
			cb(0, 0, ErrProtocol)
			return
		}
		keyLen := int(binary.BigEndian.Uint16(resp.Value[2:]))

		if len(resp.Value) != 2+2+keyLen+1+8 {
			cb(0, 0, ErrProtocol)
			return
		}
		keyState := KeyState(resp.Value[2+2+keyLen])
		cas := binary.BigEndian.Uint64(resp.Value[2+2+keyLen+1:])

		cb(keyState, Cas(cas), nil)
	}

	vbId := agent.KeyToVbucket(key)

	valueBuf := make([]byte, 2+2+len(key))
	binary.BigEndian.PutUint16(valueBuf[0:], vbId)
	binary.BigEndian.PutUint16(valueBuf[2:], uint16(len(key)))
	copy(valueBuf[4:], key)

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdObserve,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      nil,
			Value:    valueBuf,
			Vbucket:  vbId,
		},
		ReplicaIdx: replicaIdx,
		Callback:   handler,
	}
	return agent.dispatchOp(req)
}

// ObserveSeqNo retrieves the persistence state sequence numbers for a particular VBucket.
func (agent *Agent) ObserveSeqNo(key []byte, vbUuid VbUuid, replicaIdx int, cb ObserveSeqNoCallback) (PendingOp, error) {
	vbId := agent.KeyToVbucket(key)
	return agent.ObserveSeqNoEx(vbId, vbUuid, replicaIdx, func(stats *ObserveSeqNoStats, err error) {
		if err != nil {
			cb(0, 0, err)
			return
		}

		if !stats.DidFailover {
			cb(stats.CurrentSeqNo, stats.PersistSeqNo, nil)
		} else {
			cb(stats.LastSeqNo, stats.LastSeqNo, nil)
		}
	})
}

// ObserveSeqNoEx retrieves the persistence state sequence numbers for a particular VBucket
// and includes additional details not included by the basic version.
func (agent *Agent) ObserveSeqNoEx(vbId uint16, vbUuid VbUuid, replicaIdx int, cb ObserveSeqNoExCallback) (PendingOp, error) {
	// TODO(mnunberg): Use bktType when implemented
	if agent.numVbuckets == 0 {
		return nil, ErrNotSupported
	}

	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			cb(nil, err)
			return
		}

		if len(resp.Value) < 1 {
			cb(nil, ErrProtocol)
			return
		}

		formatType := resp.Value[0]
		if formatType == 0 {
			// Normal
			if len(resp.Value) < 27 {
				cb(nil, ErrProtocol)
				return
			}

			vbId := binary.BigEndian.Uint16(resp.Value[1:])
			vbUuid := binary.BigEndian.Uint64(resp.Value[3:])
			persistSeqNo := binary.BigEndian.Uint64(resp.Value[11:])
			currentSeqNo := binary.BigEndian.Uint64(resp.Value[19:])

			cb(&ObserveSeqNoStats{
				DidFailover:  false,
				VbId:         vbId,
				VbUuid:       VbUuid(vbUuid),
				PersistSeqNo: SeqNo(persistSeqNo),
				CurrentSeqNo: SeqNo(currentSeqNo),
			}, nil)
			return
		} else if formatType == 1 {
			// Hard Failover
			if len(resp.Value) < 43 {
				cb(nil, ErrProtocol)
				return
			}

			vbId := binary.BigEndian.Uint16(resp.Value[1:])
			vbUuid := binary.BigEndian.Uint64(resp.Value[3:])
			persistSeqNo := binary.BigEndian.Uint64(resp.Value[11:])
			currentSeqNo := binary.BigEndian.Uint64(resp.Value[19:])
			oldVbUuid := binary.BigEndian.Uint64(resp.Value[27:])
			lastSeqNo := binary.BigEndian.Uint64(resp.Value[35:])

			cb(&ObserveSeqNoStats{
				DidFailover:  true,
				VbId:         vbId,
				VbUuid:       VbUuid(vbUuid),
				PersistSeqNo: SeqNo(persistSeqNo),
				CurrentSeqNo: SeqNo(currentSeqNo),
				OldVbUuid:    VbUuid(oldVbUuid),
				LastSeqNo:    SeqNo(lastSeqNo),
			}, nil)
			return
		} else {
			cb(nil, ErrProtocol)
			return
		}
	}

	valueBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(valueBuf[0:], uint64(vbUuid))

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdObserveSeqNo,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      nil,
			Value:    valueBuf,
			Vbucket:  vbId,
		},
		ReplicaIdx: replicaIdx,
		Callback:   handler,
	}
	return agent.dispatchOp(req)
}
