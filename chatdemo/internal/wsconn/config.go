package wsconn

import (
	"github.com/gorilla/websocket"

	"ergo.services/ergo/gen"
)

type Config struct {
	Node        gen.Node
	RegistryPID gen.PID
	ConnID      string
	Conn        *websocket.Conn
}
