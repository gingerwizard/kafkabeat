// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

type Config struct {
	Kafkabeat KafkabeatConfig
}

type KafkabeatConfig struct {
	Period string `yaml:"period"`
	Groups [] string `yaml:"groups"`
	Topics [] string `yaml:"topics"`
	Zookeepers [] string `yaml:"zookeepers"`
}
