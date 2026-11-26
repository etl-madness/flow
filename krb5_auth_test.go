package flow

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/microsoft/go-mssqldb/msdsn"
)

func TestStripKrb5FilePrefix(t *testing.T) {
	if got := StripKrb5FilePrefix("FILE:/tmp/krb5cc_1001"); got != "/tmp/krb5cc_1001" {
		t.Fatalf("got %q, want /tmp/krb5cc_1001", got)
	}
	if got := StripKrb5FilePrefix("file: /var/db/krb5cc"); got != "/var/db/krb5cc" {
		t.Fatalf("got %q, want /var/db/krb5cc", got)
	}
	if got := StripKrb5FilePrefix("/tmp/krb5cc_1001"); got != "/tmp/krb5cc_1001" {
		t.Fatalf("got %q, want /tmp/krb5cc_1001", got)
	}
}

func TestDefaultRealmFromKrb5Conf(t *testing.T) {
	conf := filepath.Join(t.TempDir(), "krb5.conf")
	body := "# comment\n; semicolon\n[libdefaults]\n    default_realm = EXAMPLE.COM\n"
	if err := os.WriteFile(conf, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
	if got := DefaultRealmFromKrb5Conf(conf); got != "EXAMPLE.COM" {
		t.Fatalf("got %q, want EXAMPLE.COM", got)
	}
	if got := DefaultRealmFromKrb5Conf(filepath.Join(t.TempDir(), "missing.conf")); got != "" {
		t.Fatalf("expected empty realm for missing conf, got %q", got)
	}
}

func TestQualifyKerberosServerSPN(t *testing.T) {
	conf := filepath.Join(t.TempDir(), "krb5.conf")
	if err := os.WriteFile(conf, []byte("[libdefaults]\n default_realm = EXAMPLE.COM\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if got := QualifyKerberosServerSPN("MSSQLSvc/DB01:1433", "EXAMPLE.COM", ""); got != "MSSQLSvc/DB01.EXAMPLE.COM:1433" {
		t.Fatalf("got %q", got)
	}
	if got := QualifyKerberosServerSPN("MSSQLSvc/DB01:1433", "", conf); got != "MSSQLSvc/DB01.EXAMPLE.COM:1433" {
		t.Fatalf("got %q", got)
	}
	if got := QualifyKerberosServerSPN("MSSQLSvc/DB01.EXAMPLE.COM:1433", "EXAMPLE.COM", ""); got != "MSSQLSvc/DB01.EXAMPLE.COM:1433" {
		t.Fatalf("already qualified changed: %q", got)
	}
	if got := QualifyKerberosServerSPN("HTTP/web01", "EXAMPLE.COM", ""); got != "HTTP/web01" {
		t.Fatalf("non-MSSQLSvc SPN changed: %q", got)
	}
}

func TestNormalizeKrb5ConfigAndPrincipal(t *testing.T) {
	NormalizeKrb5Config(nil) // should not panic

	t.Setenv("KRB5CCNAME", "FILE:/tmp/krb5cc_999")
	t.Setenv("KRB5_KTNAME", "FILE:/etc/flow.keytab")
	t.Setenv("AD_REALM", "EXAMPLE.COM")
	t.Setenv("FLOW_WINDOWS_USER", `EXAMPLE\SVC_FLOW_API`)
	t.Setenv("KRB5_PRINCIPAL", "")
	t.Setenv("AD_USER", "")

	cfg := &msdsn.Config{ServerSPN: "MSSQLSvc/DB01:1433"}
	NormalizeKrb5Config(cfg)

	if cfg.Parameters["krb5-credcachefile"] != "/tmp/krb5cc_999" {
		t.Fatalf("credcache = %q", cfg.Parameters["krb5-credcachefile"])
	}
	if cfg.Parameters["krb5-keytabfile"] != "/etc/flow.keytab" {
		t.Fatalf("keytab = %q", cfg.Parameters["krb5-keytabfile"])
	}
	if cfg.Parameters["krb5-realm"] != "EXAMPLE.COM" {
		t.Fatalf("realm = %q", cfg.Parameters["krb5-realm"])
	}
	if cfg.User != "SVC_FLOW_API" {
		t.Fatalf("user = %q, want SVC_FLOW_API", cfg.User)
	}
	if cfg.ServerSPN != "MSSQLSvc/DB01.EXAMPLE.COM:1433" {
		t.Fatalf("ServerSPN = %q", cfg.ServerSPN)
	}

	t.Setenv("KRB5_PRINCIPAL", "u00001@EXAMPLE.COM")
	if got := ResolveKerberosPrincipalUser(); got != "u00001" {
		t.Fatalf("ResolveKerberosPrincipalUser = %q, want u00001", got)
	}
}
