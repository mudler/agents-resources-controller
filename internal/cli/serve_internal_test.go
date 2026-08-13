package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadTokens(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		wantErr string // substring expected in the error, "" means no error
		want    map[string]string
	}{
		{
			name: "valid multi-token input",
			raw:  "wtok:worker,ctok:client,atok:admin",
			want: map[string]string{"wtok": "worker", "ctok": "client", "atok": "admin"},
		},
		{
			name:    "unknown role",
			raw:     "tok:superuser",
			wantErr: `unknown role "superuser"`,
		},
		{
			name:    "malformed entry",
			raw:     "tokwithoutcolon",
			wantErr: `malformed RC_TOKENS entry "tokwithoutcolon"`,
		},
		{
			name:    "duplicate token",
			raw:     "tok:client,tok:admin",
			wantErr: `duplicate token "tok"`,
		},
		{
			name:    "duplicate token, same role",
			raw:     "tok:worker,tok:worker",
			wantErr: `duplicate token "tok"`,
		},
		{
			name:    "empty token",
			raw:     ":client",
			wantErr: `empty token`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("RC_TOKENS", tc.raw)
			got, err := loadTokens()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestLoadTokensRequiresEnvVar(t *testing.T) {
	t.Setenv("RC_TOKENS", "")
	_, err := loadTokens()
	require.ErrorContains(t, err, "RC_TOKENS required")
}
