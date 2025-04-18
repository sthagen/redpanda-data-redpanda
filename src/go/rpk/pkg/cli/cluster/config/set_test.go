package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseArgs(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		expected  []string
		expectErr bool
	}{
		{
			name:      "valid key=value format",
			args:      []string{"key=value"},
			expected:  []string{"key=value"},
			expectErr: false,
		},
		{
			name:      "valid key=value key2=value2 format",
			args:      []string{"key=value", "key2=value2"},
			expected:  []string{"key=value", "key2=value2"},
			expectErr: false,
		},
		{
			name:      "valid key and value as separate arguments",
			args:      []string{"key", "value"},
			expected:  []string{"key=value"},
			expectErr: false,
		},
		{
			name:      "invalid format without '='",
			args:      []string{"key", "value1", "value2"},
			expected:  nil,
			expectErr: true,
		},
		{
			name:      "invalid single argument without '='",
			args:      []string{"key"},
			expected:  nil,
			expectErr: true,
		},
		{
			name:      "invalid multiple arguments without '='",
			args:      []string{"key", "value1", "key", "value2"},
			expected:  nil,
			expectErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := parseArgs(test.args)
			if test.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, result)
			}
		})
	}
}
