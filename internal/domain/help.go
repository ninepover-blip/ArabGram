package domain

// AppConfig 是客户端应用配置。
type AppConfig struct {
	Client string
	Hash   int
	JSON   []byte
}

// CountryCode 是一个国家的电话区号规则。
type CountryCode struct {
	CountryCode string
	Prefixes    []string
	Patterns    []string
}

// Country 是登录页国家/区号选择项。
type Country struct {
	ISO2         string
	DefaultName  string
	Name         string
	Hidden       bool
	CountryCodes []CountryCode
}

// CountriesList 是 help.getCountriesList 查询结果。
type CountriesList struct {
	Hash      int
	Countries []Country
}
