# Go Protobuf Comment Generator

Custom `protoc` plugin that extracts proto comments and makes them available at runtime for `rpk shadow config generate-template`.

## Overview

Generates `comments.pb.go` files containing a simple `map[string]string` of proto field comments keyed by descriptor full names. Proto `.pb.go` files come from `buf.build/gen/go/redpandadata/core`.


## Usage

Generate comment files:
```bash
buf generate --path proto
```

Access comments in code:
```go
import v2comments "github.com/redpanda-data/redpanda/src/go/rpk/gen/protocomments/admin/v2"

comment := v2comments.GetCommentForField(fieldDescriptor)
// Or by full name:
comment = v2comments.GetComment("redpanda.core.admin.v2.ShadowLink.uuid")
```

## Generated Code

Each package gets one `comments.pb.go`:
```go
package v2

// Comments maps descriptor full names to their leading comments
var Comments = map[string]string{
    "redpanda.core.admin.v2.ShadowLink.uuid": "The UUID of the shadow link",
    "redpanda.core.admin.v2.ShadowLink.name": `The name of the shadow link
This is a multi-line comment`,
}

func GetComment(fullName string) string
func GetCommentForField(fd protoreflect.FieldDescriptor) string
func GetCommentForMessage(md protoreflect.MessageDescriptor) string
func GetCommentForEnum(ed protoreflect.EnumDescriptor) string
func GetCommentForEnumValue(evd protoreflect.EnumValueDescriptor) string
```

**Multi-line comments**: Use raw string literals (backticks) to preserve actual newlines instead of `\n` escape sequences.

## Output Structure

```
src/go/rpk/gen/protocomments/
├── admin/v2/comments.pb.go
└── common/v1/comments.pb.go
```

## Development

Modify plugin:
```bash
# 1. Edit generator.go
# 2. Regenerate
buf generate --path "proto/redpanda/core/admin/v2/shadow_link.proto","proto/redpanda/core/common/v1/acl.proto","proto/redpanda/core/common/v1/tls.proto"
# 3. Test
cd src/go/rpk && go build ./...
```

CI checks that generated files are up-to-date in `.github/workflows/rpk-build.yml`.
