package nodekit

import "github.com/asaidimu/hermes/pkg/core"

// SystemErrorJSON serializes an error into the TS SystemError.toJSON() shape.
// Implemented in pkg/core (pkg/pipeline needs it too, and pipeline imports
// nodekit — so it cannot live here). Re-exported for nodekit callers.
var SystemErrorJSON = core.SystemErrorJSON