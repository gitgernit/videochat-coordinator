package config

import (
	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	GRPCServerPort int `env:"GRPC_SERVER_PORT" env-default:"9090"`
}

func New() (*Config, error) {
	cfg := Config{}
	err := cleanenv.ReadConfig("./configs/.env", &cfg)

	if err != nil {
		return nil, err
	}

	return &cfg, nil
}
