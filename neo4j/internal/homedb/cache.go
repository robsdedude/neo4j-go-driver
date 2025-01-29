/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package homedb

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/auth"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/errorutil"
	"sort"
	"sync"
	"time"
)

// DefaultCacheMaxSize defines the maximum number of entries the cache can hold.
const DefaultCacheMaxSize = 1000

// cachePruneFraction defines the fraction of entries to prune when the cache exceeds its size.
const cachePruneFraction = 0.1

const keyScheme = "scheme"
const schemeNone = "none"
const schemeBasic = "basic"
const schemeKerberos = "kerberos"
const schemeBearer = "bearer"
const keyPrincipal = "principal"
const keyCredentials = "credentials"
const keyRealm = "realm"
const keyParameters = "parameters"

type cacheEntry struct {
	database string
	lastUsed time.Time
}

type Cache struct {
	maxSize int
	cache   map[string]*cacheEntry
	enabled bool
	mu      sync.RWMutex
}

// NewCache creates and returns a new cache instance with the given max size.
func NewCache(maxSize int) (*Cache, error) {
	if maxSize <= 0 {
		return nil, &errorutil.UsageError{Message: "Maximum cache size must be greater than 0"}
	}
	return &Cache{
		maxSize: maxSize,
		cache:   make(map[string]*cacheEntry),
		enabled: false,
	}, nil
}

// Get retrieves the home database for a given user, if it exists.
// It updates the `lastUsed` timestamp for the entry.
func (c *Cache) Get(user string) (string, bool) {
	if !c.IsEnabled() {
		return "", false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.cache[user]
	if !exists {
		return "", false
	}
	entry.lastUsed = time.Now()
	return entry.database, true
}

// Set adds or updates an entry in the cache.
// If the cache exceeds its max size, it prunes the least recently used entries.
func (c *Cache) Set(user string, database string) {
	if !c.IsEnabled() {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache[user] = &cacheEntry{
		database: database,
		lastUsed: time.Now(),
	}
	c.prune()
}

// Delete removes a specific user's entry from the cache.
func (c *Cache) Delete(user string) {
	if !c.IsEnabled() {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cache, user)
}

// InvalidateDatabase removes all entries that reference a specific database.
func (c *Cache) InvalidateDatabase(database string) {
	if !c.IsEnabled() {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for user, entry := range c.cache {
		if entry.database == database {
			delete(c.cache, user)
		}
	}
}

// ComputeKey generates a cache key based on user impersonation, auth token, and session context.
func (c *Cache) ComputeKey(impersonatedUser string, auth auth.Token, fromSession bool) string {
	// If impersonated user is provided, use it as the key
	if impersonatedUser != "" {
		return "basic:" + impersonatedUser
	}
	// If using driver level auth, return a default cache key to support AuthManager rotation.
	if !fromSession {
		return "DEFAULT"
	}

	// Process based on auth scheme
	if scheme, ok := auth.Tokens[keyScheme].(string); ok {
		switch scheme {
		case schemeBasic:
			if principal, ok := auth.Tokens[keyPrincipal].(string); ok {
				return "basic:" + principal
			}
			return "basic:"
		case schemeKerberos:
			if credentials, ok := auth.Tokens[keyCredentials].(string); ok {
				return "kerberos:" + credentials
			}
		case schemeBearer:
			if credentials, ok := auth.Tokens[keyCredentials].(string); ok {
				return "bearer:" + credentials
			}
		case schemeNone:
			return "none"
		default:
			// For custom schemes, construct the key
			var orderedParams string
			if params, ok := auth.Tokens[keyParameters].(map[string]any); ok {
				keys := make([]string, 0, len(params))
				for key := range params {
					keys = append(keys, key)
				}
				sort.Strings(keys)
				for _, key := range keys {
					orderedParams += key + ":" + fmt.Sprintf("%v", params[key])
				}
			}
			var credentialString, realmString string
			if credentials, ok := auth.Tokens[keyCredentials].(string); ok && credentials != "" {
				credentialString = "credentials:" + credentials
			}
			if realm, ok := auth.Tokens[keyRealm].(string); ok && realm != "" {
				realmString = "realm:" + realm
			}
			return fmt.Sprintf("%s:%s%s:%v%s%s%s:%s",
				keyScheme, scheme,
				keyPrincipal, auth.Tokens[keyPrincipal],
				credentialString, realmString,
				keyParameters, orderedParams)
		}
	}
	// If no specific key could be determined, return a default
	return "DEFAULT"
}

// SetEnabled enables or disables the cache.
func (c *Cache) SetEnabled(enabled bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.enabled = enabled
}

// IsEnabled checks whether the cache is enabled.
func (c *Cache) IsEnabled() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.enabled
}

// prune removes a chunk of the least recently used entries if the cache exceeds its max size.
func (c *Cache) prune() {
	if len(c.cache) <= c.maxSize {
		return
	}

	// Collect all entries to a slice ready to be sorted.
	entries := make([]struct {
		user  string
		entry *cacheEntry
	}, 0, len(c.cache))

	for user, entry := range c.cache {
		entries = append(entries, struct {
			user  string
			entry *cacheEntry
		}{user: user, entry: entry})
	}

	// Sort the entries by `lastUsed` (oldest first).
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].entry.lastUsed.Before(entries[j].entry.lastUsed)
	})

	// Calculate the number of entries to prune, ensuring at least one entry is removed
	pruneCount := int(float64(c.maxSize) * cachePruneFraction)
	if pruneCount == 0 {
		pruneCount = 1
	}
	// Ensure we do not prune more than the number of entries in the cache
	if pruneCount > len(entries) {
		pruneCount = len(entries)
	}

	// Remove the least recently used entries
	for i := 0; i < pruneCount; i++ {
		delete(c.cache, entries[i].user)
	}
}
