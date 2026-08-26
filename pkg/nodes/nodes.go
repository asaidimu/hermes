package nodes

import (
	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/nodes/arithmetic"
	"github.com/asaidimu/hermes/pkg/nodes/code"
	"github.com/asaidimu/hermes/pkg/nodes/database"
	"github.com/asaidimu/hermes/pkg/nodes/delay"
	"github.com/asaidimu/hermes/pkg/nodes/distribute"
	"github.com/asaidimu/hermes/pkg/nodes/for-each"
	"github.com/asaidimu/hermes/pkg/nodes/fork"
	"github.com/asaidimu/hermes/pkg/nodes/http"
	ifnode "github.com/asaidimu/hermes/pkg/nodes/if"
	"github.com/asaidimu/hermes/pkg/nodes/join"
	"github.com/asaidimu/hermes/pkg/nodes/pause"
	"github.com/asaidimu/hermes/pkg/nodes/pipeline-ref"
	"github.com/asaidimu/hermes/pkg/nodes/query"
	switchnode "github.com/asaidimu/hermes/pkg/nodes/switch"
	"github.com/asaidimu/hermes/pkg/nodes/transformer"
	"github.com/asaidimu/hermes/pkg/nodes/trigger"
	"github.com/asaidimu/hermes/pkg/nodes/try-catch"
	"github.com/asaidimu/hermes/pkg/nodes/while"
)

func init() {
	nodekit.Register(arithmetic.Node)
	nodekit.Register(code.Node)
	nodekit.Register(database.Node)
	nodekit.Register(delay.Node)
	nodekit.Register(distribute.Node)
	nodekit.Register(foreach.Node)
	nodekit.Register(fork.Node)
	nodekit.Register(http.Node)
	nodekit.Register(ifnode.Node)
	nodekit.Register(join.Node)
	nodekit.Register(pause.Node)
	nodekit.Register(pipeleref.Node)
	nodekit.Register(query.Node)
	nodekit.Register(switchnode.Node)
	nodekit.Register(transformer.Node)
	nodekit.Register(trigger.Node)
	nodekit.Register(trycatch.Node)
	nodekit.Register(while.Node)
}

// Re-export nodekit symbols for callers that import pkg/nodes.
type (
	HandleType     = nodekit.HandleType
	HandleKind     = nodekit.HandleKind
	HandleSpec     = nodekit.HandleSpec
	NodeRunContext = nodekit.NodeRunContext
	NodeDefinition = nodekit.NodeDefinition
)

var (
	Register  = nodekit.Register
	Get       = nodekit.Get
	Registry  = nodekit.Registry
	BuildStep = nodekit.BuildStep
)
