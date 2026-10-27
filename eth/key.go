package eth

import (
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"uuid"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

// Key is a local Ethereum signer. The key material never crosses the JSON-RPC
// boundary and is not included in command output or errors.
type Key struct{ private *ecdsa.PrivateKey }

// SignTx adapts the local key to the ported go-livepeer transaction signer.
func (k *Key) SignTx(tx *types.Transaction) (*types.Transaction, error) {
	return k.SignTxForChain(tx, tx.ChainId())
}

// SignTxForChain retains upstream accountManager's chain binding when an
// unsigned legacy replacement does not yet encode its original chain ID.
func (k *Key) SignTxForChain(tx *types.Transaction, chainID *big.Int) (*types.Transaction, error) {
	return types.SignTx(tx, types.LatestSignerForChainID(chainID), k.private)
}

// SelectKeystore selects metadata without unlocking a key. An explicit file
// bypasses directory discovery; its address is checked again after decryption.
func SelectKeystore(dir, explicit string, account *ethcommon.Address) (string, ethcommon.Address, error) {
	address := func(path string) (ethcommon.Address, error) {
		data, err := readOwnerOnlyFile(path, "keystore file")
		if err != nil {
			return ethcommon.Address{}, err
		}
		var metadata struct{ Address string }
		if json.Unmarshal(data, &metadata) != nil || !ethcommon.IsHexAddress(metadata.Address) {
			return ethcommon.Address{}, errors.New("invalid keystore account metadata")
		}
		return ethcommon.HexToAddress(metadata.Address), nil
	}
	if explicit != "" {
		a, err := address(explicit)
		return explicit, a, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", ethcommon.Address{}, errors.New("keystore directory is unavailable; supply --keystore-file")
	}
	var selected string
	var selectedAddress ethcommon.Address
	for _, entry := range entries {
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		a, err := address(path)
		if err != nil {
			continue
		}
		if account != nil && a != *account {
			continue
		}
		if selected != "" {
			return "", ethcommon.Address{}, errors.New("multiple keystore accounts match; supply --account or --keystore-file")
		}
		selected, selectedAddress = path, a
	}
	if selected == "" {
		return "", ethcommon.Address{}, errors.New("no matching keystore account; supply --account or --keystore-file")
	}
	return selected, selectedAddress, nil
}

// OpenAccount discovers an account and verifies its metadata against the decrypted key.
func OpenAccount(dir, path string, password *string, passwordFile string, account *ethcommon.Address) (*Key, error) {
	path, address, err := SelectKeystore(dir, path, account)
	if err != nil {
		return nil, err
	}
	key, err := OpenKeystore(path, password, passwordFile)
	if err != nil {
		return nil, err
	}
	if key.Address() != address || account != nil && key.Address() != *account {
		return nil, errors.New("configured account address does not match keystore account")
	}
	return key, nil
}

// CreateAccount saves a fresh account in the shared keystore directory.
func CreateAccount(dir string, password *string, passwordFile string) (ethcommon.Address, error) {
	if password == nil && passwordFile == "" {
		return ethcommon.Address{}, errors.New("keystore-password or keystore-password-file are required")
	}
	if _, err := readKeystorePassword(password, passwordFile); err != nil {
		return ethcommon.Address{}, err
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return ethcommon.Address{}, errors.New("keystore directory cannot be created")
	}
	return CreateKeystore(filepath.Join(dir, uuid.NewV4().String()+".json"), password, passwordFile)
}

// OpenKeystoreFile unlocks one encrypted geth account file. Password files use
// exact bytes, including whitespace; neither input is included in diagnostics.
func OpenKeystoreFile(keystorePath, passwordPath string) (*Key, error) {
	if keystorePath == "" || passwordPath == "" {
		return nil, errors.New("keystore-file and keystore-password-file are required")
	}
	return OpenKeystore(keystorePath, nil, passwordPath)
}

// OpenKeystore unlocks an encrypted geth account using password, or passwordPath
// if password is nil. Any supplied password file must be owner-only.
func OpenKeystore(keystorePath string, password *string, passwordPath string) (key *Key, err error) {
	if keystorePath == "" || password == nil && passwordPath == "" {
		return nil, errors.New("keystore-file and keystore-password or keystore-password-file are required")
	}
	data, err := readOwnerOnlyFile(keystorePath, "keystore file")
	if err != nil {
		return nil, err
	}
	auth, err := readKeystorePassword(password, passwordPath)
	if err != nil {
		return nil, err
	}
	// Geth's KDF decoder can panic on malformed JSON parameters. Sanitize both
	// panics and returned decryption errors without exposing either input.
	defer func() {
		if recover() != nil || err != nil {
			key, err = nil, errors.New("cannot decrypt keystore: invalid file or password")
		}
	}()
	decoded, err := keystore.DecryptKey(data, auth)
	if err != nil {
		return nil, err
	}
	return &Key{private: decoded.PrivateKey}, nil
}

// CreateKeystore generates an account and writes an owner-only encrypted geth
// keystore. The parent directory must exist; an existing path is never replaced.
// Password sources follow the same exact-byte rules as OpenKeystore.
func CreateKeystore(keystorePath string, password *string, passwordPath string) (ethcommon.Address, error) {
	if keystorePath == "" || password == nil && passwordPath == "" {
		return ethcommon.Address{}, errors.New("keystore-file and keystore-password or keystore-password-file are required")
	}
	if _, err := os.Lstat(keystorePath); err == nil {
		return ethcommon.Address{}, errors.New("keystore file already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return ethcommon.Address{}, errors.New("keystore file is unavailable")
	}
	auth, err := readKeystorePassword(password, passwordPath)
	if err != nil {
		return ethcommon.Address{}, err
	}
	file, err := os.CreateTemp(filepath.Dir(keystorePath), ".keystore-*")
	if err != nil {
		return ethcommon.Address{}, errors.New("keystore file cannot be created")
	}
	defer func() {
		file.Close()
		os.Remove(file.Name())
	}()
	private, err := crypto.GenerateKey()
	if err != nil {
		return ethcommon.Address{}, errors.New("cannot generate account key")
	}
	address := crypto.PubkeyToAddress(private.PublicKey)
	data, err := keystore.EncryptKey(&keystore.Key{Id: [16]byte(uuid.NewV4()), Address: address, PrivateKey: private}, auth, keystore.StandardScryptN, keystore.StandardScryptP)
	if err != nil {
		return ethcommon.Address{}, errors.New("cannot encrypt account key")
	}
	if _, err := file.Write(data); err != nil {
		return ethcommon.Address{}, errors.New("cannot write keystore file")
	}
	if err := file.Sync(); err != nil {
		return ethcommon.Address{}, errors.New("cannot sync keystore file")
	}
	if err := file.Close(); err != nil {
		return ethcommon.Address{}, errors.New("cannot close keystore file")
	}
	// Publish the complete file atomically. Link also refuses a destination
	// created after the initial check, including a dangling symlink.
	if err := os.Link(file.Name(), keystorePath); err != nil {
		if errors.Is(err, os.ErrExist) {
			return ethcommon.Address{}, errors.New("keystore file already exists")
		}
		return ethcommon.Address{}, errors.New("keystore file cannot be created")
	}
	return address, nil
}

func readKeystorePassword(password *string, passwordPath string) (string, error) {
	if password == nil {
		contents, err := readOwnerOnlyFile(passwordPath, "keystore password file")
		if err != nil {
			return "", err
		}
		defer clear(contents)
		return string(contents), nil
	}
	if passwordPath != "" {
		file, err := openOwnerOnlyFile(passwordPath, "keystore password file")
		if err != nil {
			return "", err
		}
		file.Close()
	}
	return *password, nil
}

func readOwnerOnlyFile(path, label string) ([]byte, error) {
	file, err := openOwnerOnlyFile(path, label)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("%s is unavailable", label)
	}
	return data, nil
}

func openOwnerOnlyFile(path, label string) (*os.File, error) {
	// Reject devices and FIFOs before opening; check the opened handle as well
	// so permissions and type checks apply to the file actually read.
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("%s is unavailable", label)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s must be a regular file", label)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%s is unavailable", label)
	}
	info, err = file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("%s is unavailable", label)
	}
	if !info.Mode().IsRegular() {
		file.Close()
		return nil, fmt.Errorf("%s must be a regular file", label)
	}
	if info.Mode().Perm()&0077 != 0 {
		file.Close()
		return nil, fmt.Errorf("%s must be owner-only", label)
	}
	return file, nil
}

func (k *Key) Address() ethcommon.Address { return crypto.PubkeyToAddress(k.private.PublicKey) }

// Ethereum message signing and the 27/28 convention follow Yondon Fu's
// go-livepeer/eth/accountmanager.go (3b52399fc7b9177460864207a68eb9f3d2f4ee57).
func (k *Key) SignMessage(message []byte) ([]byte, error) {
	return k.SignHash(accounts.TextHash(message))
}

// Hash signing follows Yondon Fu's go-livepeer/eth/accountmanager.go signHash
// (97a014cb692a736a853f7ebb396e9cbada5408a6).
func (k *Key) SignHash(hash []byte) ([]byte, error) {
	if len(hash) != 32 {
		return nil, errors.New("ethereum signing hash must be 32 bytes")
	}
	sig, err := crypto.Sign(hash, k.private)
	if err != nil {
		return nil, err
	}
	sig[64] += 27
	return sig, nil
}

func HexSignature(sig []byte) string { return hexutil.Encode(sig) }
