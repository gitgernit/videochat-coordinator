package config

import (
	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	GRPCServerHost string `env:"GRPC_SERVER_HOST" env-default:"localhost"`
	GRPCServerPort int    `env:"GRPC_SERVER_PORT" env-default:"9090"`
}

func New() (*Config, error) {
	cfg := Config{}
	err := cleanenv.ReadEnv(&cfg)

	if err != nil {
		return nil, err
	}

	return &cfg, nil
}
