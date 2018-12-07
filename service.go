package simpleraft

type Command interface {
}

type CommandCoder interface {
	Encode(Command) ([]byte, error)
	Decode([]byte) (Command, error)
}

type Service interface {
	Close() error
	Restore(data []byte) error
	Backup() ([]byte, error)
	HandleCmd(Command) (interface{}, error)
	IsCmdReadOnly(Command) bool
}
