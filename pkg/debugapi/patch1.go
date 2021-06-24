package debugapi

import (
	"encoding/hex"
	"net/http"
	"path/filepath"
	"time"

	"github.com/ethersphere/bee/pkg/logging"
	"github.com/ethersphere/bee/pkg/resolver/multiresolver"

	"github.com/ethereum/go-ethereum/common"
	"github.com/multiformats/go-multiaddr"

	"github.com/ethersphere/bee/pkg/crypto"
	"github.com/ethersphere/bee/pkg/jsonhttp"
)

type Options struct {
	DataDir                    string
	CacheCapacity              uint64
	DBOpenFilesLimit           uint64
	DBWriteBufferSize          uint64
	DBBlockCacheCapacity       uint64
	DBDisableSeeksCompaction   bool
	APIAddr                    string
	DebugAPIAddr               string
	Addr                       string
	NATAddr                    string
	EnableWS                   bool
	EnableQUIC                 bool
	WelcomeMessage             string
	Bootnodes                  []string
	CORSAllowedOrigins         []string
	Logger                     logging.Logger
	Standalone                 bool
	TracingEnabled             bool
	TracingEndpoint            string
	TracingServiceName         string
	GlobalPinningEnabled       bool
	PaymentThreshold           string
	PaymentTolerance           string
	PaymentEarly               string
	ResolverConnectionCfgs     []multiresolver.ConnectionConfig
	GatewayMode                bool
	BootnodeMode               bool
	SwapEndpoint               string
	SwapFactoryAddress         string
	SwapLegacyFactoryAddresses []string
	SwapInitialDeposit         string
	SwapEnable                 bool
	FullNodeMode               bool
	Transaction                string
	BlockHash                  string
	PostageContractAddress     string
	PriceOracleAddress         string
	BlockTime                  uint64
	DeployGasPrice             string
	WarmupTime                 time.Duration
}

var Opt Options
var ChainId int64

type addresses3Response struct {
	Overlay      string                `json:"overlay"`
	Underlay     []multiaddr.Multiaddr `json:"underlay"`
	Ethereum     common.Address        `json:"ethereum"`
	PublicKey    string                `json:"publicKey"`
	PSSPublicKey string                `json:"pssPublicKey"`
	Bootmode     bool                  `json:"bootmode"`
	DataDir      string                `json:"data_dir"`
	ChainId      int                   `json:"chain_id"`
}

func (s *Service) addresses3Handler(w http.ResponseWriter, r *http.Request) {
	var overlay string
	if s.overlay != nil {
		overlay = s.overlay.String()
	}
	datadir, _ := filepath.Abs(Opt.DataDir)
	jsonhttp.OK(w, addresses3Response{
		Overlay:      overlay,
		Ethereum:     s.ethereumAddress,
		PublicKey:    hex.EncodeToString(crypto.EncodeSecp256k1PublicKey(&s.publicKey)),
		PSSPublicKey: hex.EncodeToString(crypto.EncodeSecp256k1PublicKey(&s.pssPublicKey)),
		Bootmode:     Opt.BootnodeMode,
		DataDir:      datadir,
		ChainId:      int(ChainId),
	})
}
