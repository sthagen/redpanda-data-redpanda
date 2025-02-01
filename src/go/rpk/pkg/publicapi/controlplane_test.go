package publicapi

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPaginate(t *testing.T) {
	tests := []struct {
		name      string
		maxPages  int
		fetchPage func(ctx context.Context, pageToken string) ([]int, string, error)
		exp       []int
		expErr    bool
	}{
		{
			name:     "successful single page",
			maxPages: 3,
			fetchPage: func(ctx context.Context, pageToken string) ([]int, string, error) {
				return []int{1, 2, 3}, "", nil
			},
			exp: []int{1, 2, 3},
		},
		{
			name:     "successful multiple pages",
			maxPages: 3,
			fetchPage: func(ctx context.Context, pageToken string) ([]int, string, error) {
				switch pageToken {
				case "":
					return []int{1, 2}, "page2", nil
				case "page2":
					return []int{3, 4}, "page3", nil
				case "page3":
					return []int{5}, "", nil
				default:
					return nil, "", errors.New("unexpected page token")
				}
			},
			exp: []int{1, 2, 3, 4, 5},
		},
		{
			name:     "error in fetchPage",
			maxPages: 3,
			fetchPage: func(ctx context.Context, pageToken string) ([]int, string, error) {
				return nil, "", errors.New("fetch error")
			},
			expErr: true,
		},
		{
			name:     "exceeds maxPages",
			maxPages: 2,
			fetchPage: func(ctx context.Context, pageToken string) ([]int, string, error) {
				switch pageToken {
				case "":
					return []int{1, 2}, "page2", nil
				case "page2":
					return []int{3, 4}, "page3", nil
				default:
					return nil, "", errors.New("unexpected page token")
				}
			},
			expErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			items, err := Paginate(context.Background(), tt.maxPages, tt.fetchPage)
			if tt.expErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.exp, items)
		})
	}
}
