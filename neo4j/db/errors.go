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

package db

import (
	"fmt"
	"reflect"
	"strings"
)

type ErrorClassification string

const (
	ClientError    ErrorClassification = "CLIENT_ERROR"
	DatabaseError  ErrorClassification = "DATABASE_ERROR"
	TransientError ErrorClassification = "TRANSIENT_ERROR"
	UnknownError   ErrorClassification = "UNKNOWN"
)

// TODO: when we no longer need to support the old Neo4jError, rename the following fields:
// - Neo4jError -> GqlError
// - GqlStatusDescription -> StatusDescription
// - GqlClassification -> Classification
// - GqlRawClassification -> RawClassification
// - GqlDiagnosticRecord -> DiagnosticRecord
// - GqlCause -> Cause
// - GqlStatus to remain as-is to comply with GQLSTATUS.

// Neo4jError is created when the database server fails to fulfill a request.
type Neo4jError struct {
	// Code is the Neo4j-specific error code, to be deprecated in favor of GqlStatus.
	Code string
	// Msg is the specific error message describing the failure.
	Msg string
	// GqlStatus returns the GQLSTATUS.
	// GqlStatus is the error code compliant with the GQL specification.
	//
	// GqlStatus is part of the GQL compliant errors preview feature
	// (see README on what it means in terms of support and compatibility guarantees)
	GqlStatus string
	// GqlStatusDescription provides a standard description for the associated GQLStatus code.
	//
	// GqlStatusDescription is part of the GQL compliant errors preview feature
	// (see README on what it means in terms of support and compatibility guarantees)
	GqlStatusDescription string
	// GqlClassification is a high-level categorization of the error, specific to GQL error handling.
	//
	// GqlClassification is part of the GQL compliant errors preview feature
	// (see README on what it means in terms of support and compatibility guarantees)
	GqlClassification ErrorClassification
	// GqlRawClassification holds the raw classification as received from the server.
	//
	// GqlRawClassification is part of the GQL compliant errors preview feature
	// (see README on what it means in terms of support and compatibility guarantees)
	GqlRawClassification string
	// GqlDiagnosticRecord returns further information about the status for diagnostic purposes.
	//
	// GqlDiagnosticRecord is part of the GQL compliant errors preview feature
	// (see README on what it means in terms of support and compatibility guarantees)
	GqlDiagnosticRecord map[string]any
	// GqlCause represents the underlying error, if any, which caused the current error.
	//
	// GqlCause is part of the GQL compliant errors preview feature
	// (see README on what it means in terms of support and compatibility guarantees)
	GqlCause       *Neo4jError
	parsed         bool
	classification string // Legacy non-GQL classification
	category       string
	title          string
	retriable      bool
}

func (e *Neo4jError) Error() string {
	return fmt.Sprintf("Neo4jError: %s (%s)", e.Code, e.Msg)
}

// TODO 6.0: remove in favour of GqlClassification
func (e *Neo4jError) Classification() string {
	e.parse()
	return e.classification
}

func (e *Neo4jError) Category() string {
	e.parse()
	return e.category
}

func (e *Neo4jError) Title() string {
	e.parse()
	return e.title
}

// parse code from Neo4j into usable parts.
// Code Neo.ClientError.General.ForbiddenReadOnlyDatabase is split into:
//
//	Classification: ClientError
//	Category: General
//	Title: ForbiddenReadOnlyDatabase
func (e *Neo4jError) parse() {
	if e.parsed {
		return
	}
	e.parsed = true
	e.reclassify()
	parts := strings.Split(e.Code, ".")
	if len(parts) != 4 {
		return
	}
	e.classification = parts[1]
	e.category = parts[2]
	e.title = parts[3]
}

// reclassify classifies specific errors coming from pre-5.x servers into their
// 5.x classifications
// this function can be removed once support for pre-5.x servers is dropped
func (e *Neo4jError) reclassify() {
	switch e.Code {
	case "Neo.TransientError.Transaction.LockClientStopped":
		e.Code = "Neo.ClientError.Transaction.LockClientStopped"
	case "Neo.TransientError.Transaction.Terminated":
		e.Code = "Neo.ClientError.Transaction.Terminated"
	}
}

func (e *Neo4jError) HasSecurityCode() bool {
	return strings.HasPrefix(e.Code, "Neo.ClientError.Security.")
}

func (e *Neo4jError) IsAuthenticationFailed() bool {
	return e.Code == "Neo.ClientError.Security.Unauthorized"
}

func (e *Neo4jError) IsRetriable() bool {
	return e.retriable ||
		e.IsRetriableTransient() ||
		e.IsRetriableCluster() ||
		e.Code == "Neo.ClientError.Security.AuthorizationExpired"
}

func (e *Neo4jError) IsRetriableTransient() bool {
	e.parse()
	return e.classification == "TransientError"
}

func (e *Neo4jError) IsRetriableCluster() bool {
	switch e.Code {
	case "Neo.ClientError.Cluster.NotALeader", "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase":
		return true
	}
	return false
}

func (e *Neo4jError) MarkRetriable() {
	e.retriable = true
}

type FeatureNotSupportedError struct {
	Server  string
	Feature string
	Reason  string
}

func (e *FeatureNotSupportedError) Error() string {
	return fmt.Sprintf("Server %s does not support: %s (%s)", e.Server, e.Feature, e.Reason)
}

type UnsupportedTypeError struct {
	Type reflect.Type
}

func (e *UnsupportedTypeError) Error() string {
	return fmt.Sprintf("Usage of type '%s' is not supported", e.Type.String())
}

type ProtocolError struct {
	MessageType string
	Field       string
	Err         string
}

func (e *ProtocolError) Error() string {
	if e.MessageType == "" {
		return fmt.Sprintf("ProtocolError: %s", e.Err)
	}
	if e.Field == "" {
		return fmt.Sprintf("ProtocolError: message %s could not be hydrated: %s", e.MessageType, e.Err)
	}
	return fmt.Sprintf("ProtocolError: field %s of message %s could not be hydrated: %s",
		e.Field, e.MessageType, e.Err)
}
