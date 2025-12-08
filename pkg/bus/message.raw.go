package bus

type RawMessage struct {
	Topic   string
	Id      string
	Payload []byte
}

func (r *RawMessage) GetId() string { return r.Id }

func (r *RawMessage) TypeKey() string { return r.Topic }

func (r *RawMessage) Bytes() ([]byte, error) { return r.Payload, nil }

func (r *RawMessage) Raw() any { return r.Payload }

func (r *RawMessage) Unmarshal(data []byte) error {
	r.Payload = data
	return nil
}
