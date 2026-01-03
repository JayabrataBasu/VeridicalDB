// Package auth provides user authentication and authorization for VeridicalDB.
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/crypto/bcrypt"
)

var (
	// ErrUserNotFound is returned when a user doesn't exist.
	ErrUserNotFound = errors.New("user not found")
	// ErrUserExists is returned when trying to create an existing user.
	ErrUserExists = errors.New("user already exists")
	// ErrInvalidPassword is returned when password doesn't match.
	ErrInvalidPassword = errors.New("invalid password")
	// ErrAccessDenied is returned when user lacks required privilege.
	ErrAccessDenied = errors.New("access denied")
)

// User represents a database user.
type User struct {
	Username     string            `json:"username"`
	PasswordHash string            `json:"password_hash"`
	Salt         string            `json:"salt"`
	Superuser    bool              `json:"superuser"`
	Privileges   map[string][]Priv `json:"privileges"` // table -> privileges
}

// Priv represents a privilege type.
type Priv string

const (
	PrivSelect Priv = "SELECT"
	PrivInsert Priv = "INSERT"
	PrivUpdate Priv = "UPDATE"
	PrivDelete Priv = "DELETE"
	PrivAll    Priv = "ALL"
)

// UserCatalog manages user accounts and authentication.
type UserCatalog struct {
	mu       sync.RWMutex
	users    map[string]*User
	dataDir  string
	filePath string
}

// generateRandomPassword returns a random password hex string of n bytes (2n hex chars).
func generateRandomPassword(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// NewUserCatalog creates a new user catalog.
func NewUserCatalog(dataDir string) (*UserCatalog, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	uc := &UserCatalog{
		users:    make(map[string]*User),
		dataDir:  dataDir,
		filePath: filepath.Join(dataDir, "users.json"),
	}

	// Load existing users
	if err := uc.load(); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("load users: %w", err)
	}

	// Create default admin user if no users exist
	if len(uc.users) == 0 {
		adminPw := os.Getenv("VERIDICALDB_DEFAULT_ADMIN_PASSWORD")
		if adminPw == "" {
			pw, err := generateRandomPassword(12)
			if err != nil {
				return nil, fmt.Errorf("generate default admin password: %w", err)
			}
			// Inform operator of generated password
			fmt.Printf("Created default admin user 'admin' with password: %s\n", pw)
			adminPw = pw
		}

		if err := uc.CreateUser("admin", adminPw, true); err != nil {
			return nil, fmt.Errorf("create default admin: %w", err)
		}
	}

	return uc, nil
}

// load reads users from disk.
func (uc *UserCatalog) load() error {
	data, err := os.ReadFile(uc.filePath)
	if err != nil {
		return err
	}

	var users []*User
	if err := json.Unmarshal(data, &users); err != nil {
		return err
	}

	for _, u := range users {
		uc.users[u.Username] = u
	}
	return nil
}

// save writes users to disk.
func (uc *UserCatalog) save() error {
	users := make([]*User, 0, len(uc.users))
	for _, u := range uc.users {
		users = append(users, u)
	}

	data, err := json.MarshalIndent(users, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(uc.filePath, data, 0600)
}

// legacyHashPassword creates a salted SHA256 hash of the password (legacy behavior).
func legacyHashPassword(password, salt string) string {
	h := sha256.New()
	h.Write([]byte(salt + password))
	return hex.EncodeToString(h.Sum(nil))
}

// Note: legacy salt generator removed; legacy accounts are migrated on first login and
// new accounts use bcrypt without salts.

// bcrypt cost to use for new password hashes.
const bcryptCost = 12

// generateBcryptHash creates a bcrypt hashed password for storage.
func generateBcryptHash(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

// compareBcryptHash verifies a bcrypt hash against a plaintext password.
func compareBcryptHash(hash, password string) error {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
}

// isLegacyHash determines whether a stored password looks like the legacy SHA256 hex hash.
func isLegacyHash(hash string, salt string) bool {
	// legacy hash is hex encoded 32 bytes -> 64 chars and salt is non-empty
	if salt == "" {
		return false
	}
	if len(hash) != 64 {
		return false
	}
	// ensure it's valid hex
	_, err := hex.DecodeString(hash)
	return err == nil
}

// CreateUser creates a new user.
func (uc *UserCatalog) CreateUser(username, password string, superuser bool) error {
	uc.mu.Lock()
	defer uc.mu.Unlock()

	if _, exists := uc.users[username]; exists {
		return ErrUserExists
	}

	// Generate bcrypt hash for new users
	hash, err := generateBcryptHash(password)
	if err != nil {
		return fmt.Errorf("generate bcrypt hash: %w", err)
	}

	user := &User{
		Username:     username,
		PasswordHash: hash,
		Salt:         "", // no salt needed for bcrypt
		Superuser:    superuser,
		Privileges:   make(map[string][]Priv),
	}

	uc.users[username] = user
	return uc.save()
}

// DropUser removes a user.
func (uc *UserCatalog) DropUser(username string, ifExists bool) error {
	uc.mu.Lock()
	defer uc.mu.Unlock()

	if _, exists := uc.users[username]; !exists {
		if ifExists {
			return nil
		}
		return ErrUserNotFound
	}

	delete(uc.users, username)
	return uc.save()
}

// AlterPassword changes a user's password.
func (uc *UserCatalog) AlterPassword(username, newPassword string) error {
	uc.mu.Lock()
	defer uc.mu.Unlock()

	user, exists := uc.users[username]
	if !exists {
		return ErrUserNotFound
	}

	// Generate bcrypt hash for changed password
	hash, err := generateBcryptHash(newPassword)
	if err != nil {
		return fmt.Errorf("generate bcrypt hash: %w", err)
	}

	user.Salt = ""
	user.PasswordHash = hash
	return uc.save()
}

// SetSuperuser sets or unsets superuser status.
func (uc *UserCatalog) SetSuperuser(username string, superuser bool) error {
	uc.mu.Lock()
	defer uc.mu.Unlock()

	user, exists := uc.users[username]
	if !exists {
		return ErrUserNotFound
	}

	user.Superuser = superuser
	return uc.save()
}

// Authenticate verifies username and password.
// If the user has a legacy SHA-256 password, upgrade it to bcrypt on successful auth.
func (uc *UserCatalog) Authenticate(username, password string) (*User, error) {
	uc.mu.Lock()
	defer uc.mu.Unlock()

	user, exists := uc.users[username]
	if !exists {
		return nil, ErrUserNotFound
	}

	// Legacy SHA-256 + salt path: verify and migrate
	if isLegacyHash(user.PasswordHash, user.Salt) {
		if legacyHashPassword(password, user.Salt) != user.PasswordHash {
			return nil, ErrInvalidPassword
		}
		// Migrate to bcrypt
		hash, err := generateBcryptHash(password)
		if err != nil {
			return nil, fmt.Errorf("upgrade password to bcrypt: %w", err)
		}
		user.PasswordHash = hash
		user.Salt = ""
		if err := uc.save(); err != nil {
			return nil, fmt.Errorf("save migrated user: %w", err)
		}
		return user, nil
	}

	// bcrypt verification
	if err := compareBcryptHash(user.PasswordHash, password); err != nil {
		return nil, ErrInvalidPassword
	}

	return user, nil
}

// GetUser returns a user by username.
func (uc *UserCatalog) GetUser(username string) (*User, error) {
	uc.mu.RLock()
	defer uc.mu.RUnlock()

	user, exists := uc.users[username]
	if !exists {
		return nil, ErrUserNotFound
	}
	return user, nil
}

// ListUsers returns all usernames.
func (uc *UserCatalog) ListUsers() []string {
	uc.mu.RLock()
	defer uc.mu.RUnlock()

	names := make([]string, 0, len(uc.users))
	for name := range uc.users {
		names = append(names, name)
	}
	return names
}

// Grant grants a privilege on a table to a user.
func (uc *UserCatalog) Grant(username, tableName string, priv Priv) error {
	uc.mu.Lock()
	defer uc.mu.Unlock()

	user, exists := uc.users[username]
	if !exists {
		return ErrUserNotFound
	}

	if user.Privileges == nil {
		user.Privileges = make(map[string][]Priv)
	}

	// Check if already granted
	for _, p := range user.Privileges[tableName] {
		if p == priv || p == PrivAll {
			return nil // Already has privilege
		}
	}

	user.Privileges[tableName] = append(user.Privileges[tableName], priv)
	return uc.save()
}

// Revoke revokes a privilege on a table from a user.
func (uc *UserCatalog) Revoke(username, tableName string, priv Priv) error {
	uc.mu.Lock()
	defer uc.mu.Unlock()

	user, exists := uc.users[username]
	if !exists {
		return ErrUserNotFound
	}

	privs := user.Privileges[tableName]
	newPrivs := make([]Priv, 0, len(privs))
	for _, p := range privs {
		if p != priv {
			newPrivs = append(newPrivs, p)
		}
	}
	user.Privileges[tableName] = newPrivs
	return uc.save()
}

// HasPrivilege checks if a user has a specific privilege on a table.
func (uc *UserCatalog) HasPrivilege(username, tableName string, priv Priv) bool {
	uc.mu.RLock()
	defer uc.mu.RUnlock()

	user, exists := uc.users[username]
	if !exists {
		return false
	}

	// Superusers have all privileges
	if user.Superuser {
		return true
	}

	for _, p := range user.Privileges[tableName] {
		if p == priv || p == PrivAll {
			return true
		}
	}
	return false
}

// CheckAccess verifies a user has the required privilege, returning an error if not.
func (uc *UserCatalog) CheckAccess(username, tableName string, priv Priv) error {
	if !uc.HasPrivilege(username, tableName, priv) {
		return fmt.Errorf("%w: user %q lacks %s on %s", ErrAccessDenied, username, priv, tableName)
	}
	return nil
}
