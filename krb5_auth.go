package flow

import (
	"os"
	"runtime"
	"strings"

	"github.com/microsoft/go-mssqldb/integratedauth"
	"github.com/microsoft/go-mssqldb/integratedauth/krb5"
	"github.com/microsoft/go-mssqldb/msdsn"
)

func init() {
	ConfigureUnixKerberosSQLServerAuth()
}

// ConfigureUnixKerberosSQLServerAuth registers a smart integrated authenticator on non-Windows platforms
// (Linux/macOS/FreeBSD/OpenBSD) that normalizes KRB5CCNAME ("FILE:/path" -> "/path") for gokrb5 and
// uses Kerberos (krb5) whenever a credential cache, keytab, or non-NTLM integrated security connection is requested.
func ConfigureUnixKerberosSQLServerAuth() {
	if runtime.GOOS == "windows" {
		return
	}
	ntlmProvider, _ := getRegisteredAuthProvider("ntlm")
	_ = integratedauth.SetIntegratedAuthenticationProvider("krb5", integratedauth.ProviderFunc(func(cfg msdsn.Config) (integratedauth.IntegratedAuthenticator, error) {
		NormalizeKrb5Config(&cfg)
		return krb5.AuthProviderFunc.GetIntegratedAuthenticator(cfg)
	}))
	_ = integratedauth.SetIntegratedAuthenticationProvider("smart_unix", integratedauth.ProviderFunc(func(cfg msdsn.Config) (integratedauth.IntegratedAuthenticator, error) {
		return smartUnixAuthenticator(cfg, ntlmProvider)
	}))
	integratedauth.DefaultProviderName = "smart_unix"
}

func smartUnixAuthenticator(cfg msdsn.Config, ntlmProvider integratedauth.Provider) (integratedauth.IntegratedAuthenticator, error) {
	// If DOMAIN\user + password is provided, use NTLM
	if strings.ContainsRune(cfg.User, '\\') && cfg.Password != "" && ntlmProvider != nil {
		return ntlmProvider.GetIntegratedAuthenticator(cfg)
	}
	// Bare username + password (e.g. SQL Server login like "sa") without an explicit
	// authenticator=krb5 parameter must fall back to SQL Server Authentication rather
	// than attempting a Kerberos AS-REQ with the SQL username.
	if cfg.User != "" && cfg.Password != "" {
		return nil, nil
	}
	NormalizeKrb5Config(&cfg)
	return krb5.AuthProviderFunc.GetIntegratedAuthenticator(cfg)
}

func getRegisteredAuthProvider(name string) (integratedauth.Provider, error) {
	return integratedauth.ProviderFunc(func(cfg msdsn.Config) (integratedauth.IntegratedAuthenticator, error) {
		clone := cfg
		clone.Parameters = make(map[string]string, len(cfg.Parameters)+1)
		for k, v := range cfg.Parameters {
			clone.Parameters[k] = v
		}
		clone.Parameters["authenticator"] = name
		return integratedauth.GetIntegratedAuthenticator(clone)
	}), nil
}

// NormalizeKrb5Config strips the MIT Kerberos "FILE:" prefix from KRB5CCNAME / krb5-credcachefile
// because go-mssqldb's krb5 package passes the path directly to os.Stat() and credentials.LoadCCache(),
// and qualifies short hostnames in ServerSPN (e.g., MSSQLSvc/T15P:1433 -> MSSQLSvc/T15P.EXAMPLE.COM:1433)
// so Active Directory KDCs can locate the registered SQL Server SPN.
func NormalizeKrb5Config(cfg *msdsn.Config) {
	if cfg == nil {
		return
	}
	if cfg.Parameters == nil {
		cfg.Parameters = make(map[string]string)
	}
	cc := strings.TrimSpace(cfg.Parameters["krb5-credcachefile"])
	if cc == "" {
		cc = strings.TrimSpace(os.Getenv("KRB5CCNAME"))
	}
	if cleaned := StripKrb5FilePrefix(cc); cleaned != "" {
		cfg.Parameters["krb5-credcachefile"] = cleaned
	}
	kt := strings.TrimSpace(cfg.Parameters["krb5-keytabfile"])
	if kt == "" {
		kt = strings.TrimSpace(os.Getenv("KRB5_KTNAME"))
	}
	if cleaned := StripKrb5FilePrefix(kt); cleaned != "" {
		cfg.Parameters["krb5-keytabfile"] = cleaned
	}
	if strings.TrimSpace(cfg.Parameters["krb5-realm"]) == "" {
		if envRealm := strings.TrimSpace(os.Getenv("AD_REALM")); envRealm != "" {
			cfg.Parameters["krb5-realm"] = envRealm
		}
	}
	if cfg.Parameters["krb5-keytabfile"] != "" && strings.TrimSpace(cfg.User) == "" {
		if principal := ResolveKerberosPrincipalUser(); principal != "" {
			cfg.User = principal
		}
	}
	cfg.ServerSPN = QualifyKerberosServerSPN(cfg.ServerSPN, cfg.Parameters["krb5-realm"], cfg.Parameters["krb5-configfile"])
}

// ResolveKerberosPrincipalUser extracts a bare Kerberos username from KRB5_PRINCIPAL,
// AD_USER, FLOW_WINDOWS_USER, or HOST_USERNAME.
func ResolveKerberosPrincipalUser() string {
	for _, candidate := range []string{
		os.Getenv("KRB5_PRINCIPAL"),
		os.Getenv("AD_USER"),
		os.Getenv("FLOW_WINDOWS_USER"),
		os.Getenv("HOST_USERNAME"),
	} {
		v := strings.TrimSpace(candidate)
		if v == "" {
			continue
		}
		if slashIdx := strings.LastIndexAny(v, `\/`); slashIdx >= 0 {
			v = strings.TrimSpace(v[slashIdx+1:])
		}
		if atIdx := strings.IndexByte(v, '@'); atIdx > 0 {
			v = strings.TrimSpace(v[:atIdx])
		}
		if v != "" {
			return v
		}
	}
	return ""
}

// QualifyKerberosServerSPN appends the realm domain to unqualified MSSQLSvc/<host> SPNs.
func QualifyKerberosServerSPN(spn, explicitRealm, explicitConf string) string {
	spn = strings.TrimSpace(spn)
	if !strings.HasPrefix(spn, "MSSQLSvc/") {
		return spn
	}
	rest := spn[len("MSSQLSvc/"):]
	host := rest
	suffix := ""
	if colonIdx := strings.IndexByte(rest, ':'); colonIdx >= 0 {
		host = rest[:colonIdx]
		suffix = rest[colonIdx:]
	}
	if host == "" || strings.Contains(host, ".") {
		return spn
	}
	domain := strings.TrimSpace(explicitRealm)
	if domain == "" {
		domain = strings.TrimSpace(os.Getenv("AD_REALM"))
	}
	if domain == "" {
		domain = DefaultRealmFromKrb5Conf(explicitConf)
	}
	if domain == "" || !strings.Contains(domain, ".") {
		return spn
	}
	return "MSSQLSvc/" + host + "." + domain + suffix
}

// DefaultRealmFromKrb5Conf parses default_realm from the given krb5.conf path (or KRB5_CONFIG / /etc/krb5.conf).
func DefaultRealmFromKrb5Conf(confPath string) string {
	if strings.TrimSpace(confPath) == "" {
		confPath = strings.TrimSpace(os.Getenv("KRB5_CONFIG"))
	}
	if confPath == "" {
		confPath = "/etc/krb5.conf"
	}
	data, err := os.ReadFile(confPath)
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, ";") {
			continue
		}
		if kv := strings.SplitN(trimmed, "=", 2); len(kv) == 2 && strings.EqualFold(strings.TrimSpace(kv[0]), "default_realm") {
			return strings.TrimSpace(kv[1])
		}
	}
	return ""
}

// StripKrb5FilePrefix strips a leading case-insensitive "FILE:" prefix from a Kerberos cache or keytab path.
func StripKrb5FilePrefix(val string) string {
	v := strings.TrimSpace(val)
	if len(v) >= 5 && strings.EqualFold(v[:5], "FILE:") {
		return strings.TrimSpace(v[5:])
	}
	return v
}
