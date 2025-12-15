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
		if err := uc.CreateUser("admin", "admin", true); err != nil {
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

// hashPassword creates a salted hash of the password.
func hashPassword(password, salt string) string {
	h := sha256.New()
	h.Write([]byte(salt + password))
	return hex.EncodeToString(h.Sum(nil))
}

// generateSalt creates a random salt.
func generateSalt() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// CreateUser creates a new user.
func (uc *UserCatalog) CreateUser(username, password string, superuser bool) error {
	uc.mu.Lock()
	defer uc.mu.Unlock()

	if _, exists := uc.users[username]; exists {
		return ErrUserExists
	}

	salt, err := generateSalt()
	if err != nil {
		return fmt.Errorf("generate salt: %w", err)
	}

	user := &User{
		Username:     username,
		PasswordHash: hashPassword(password, salt),
		Salt:         salt,
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

	salt, err := generateSalt()
	if err != nil {
		return fmt.Errorf("generate salt: %w", err)
	}

	user.Salt = salt
	user.PasswordHash = hashPassword(newPassword, salt)
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
func (uc *UserCatalog) Authenticate(username, password string) (*User, error) {
	uc.mu.RLock()
	defer uc.mu.RUnlock()

	user, exists := uc.users[username]
	if !exists {
		return nil, ErrUserNotFound
	}

	if hashPassword(password, user.Salt) != user.PasswordHash {
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
