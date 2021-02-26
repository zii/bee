// Copyright 2019 The Swarm Authors
// This file is part of the Swarm library.
//
// The Swarm library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The Swarm library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the Swarm library. If not, see <http://www.gnu.org/licenses/>.

package localstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/ethersphere/bee/pkg/shed"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
)

var errMissingCurrentSchema = errors.New("could not find current db schema")
var errMissingTargetSchema = errors.New("could not find target db schema")

type migration struct {
	name string             // name of the schema
	fn   func(db *DB) error // the migration function that needs to be performed in order to get to the current schema name
}

// schemaMigrations contains an ordered list of the database schemes, that is
// in order to run data migrations in the correct sequence
var schemaMigrations = []migration{
	{name: DbSchemaCode, fn: func(db *DB) error { return nil }},
}

func (db *DB) migrate(schemaName string) error {
	migrations, err := getMigrations(schemaName, DbSchemaCurrent, schemaMigrations, db)
	if err != nil {
		return fmt.Errorf("error getting migrations for current schema (%s): %v", schemaName, err)
	}

	// no migrations to run
	if migrations == nil {
		return nil
	}

	db.logger.Infof("localstore migration: need to run %v data migrations on localstore to schema %s", len(migrations), schemaName)
	for i := 0; i < len(migrations); i++ {
		err := migrations[i].fn(db)
		if err != nil {
			return err
		}
		err = db.schemaName.Put(migrations[i].name) // put the name of the current schema
		if err != nil {
			return err
		}
		schemaName, err = db.schemaName.Get()
		if err != nil {
			return err
		}
		db.logger.Infof("localstore migration: successfully ran migration: id %v current schema: %s", i, schemaName)
	}
	return nil
}

// getMigrations returns an ordered list of migrations that need be executed
// with no errors in order to bring the localstore to the most up-to-date
// schema definition
func getMigrations(currentSchema, targetSchema string, allSchemeMigrations []migration, db *DB) (migrations []migration, err error) {
	foundCurrent := false
	foundTarget := false
	if currentSchema == DbSchemaCurrent {
		return nil, nil
	}
	for i, v := range allSchemeMigrations {
		switch v.name {
		case currentSchema:
			if foundCurrent {
				return nil, errors.New("found schema name for the second time when looking for migrations")
			}
			foundCurrent = true
			db.logger.Infof("localstore migration: found current localstore schema %s, migrate to %s, total migrations %d", currentSchema, DbSchemaCurrent, len(allSchemeMigrations)-i)
			continue // current schema migration should not be executed (already has been when schema was migrated to)
		case targetSchema:
			foundTarget = true
		}
		if foundCurrent {
			migrations = append(migrations, v)
		}
	}
	if !foundCurrent {
		return nil, errMissingCurrentSchema
	}
	if !foundTarget {
		return nil, errMissingTargetSchema
	}
	return migrations, nil
}

func gcAll(db *DB) {
	page := 1000
	ctx := context.Background()
	done := 0
	i := 0
	var addrs []swarm.Address
	err := db.retrievalDataIndex.Iterate(func(item shed.Item) (stop bool, err error) {
		addr := swarm.NewAddress(item.Address)
		addrs = append(addrs, addr)
		i++
		if i == page {
			err = db.Set(ctx, storage.ModeSetSync, addrs...)
			if err != nil {
				return true, err
			}
			done += i
			db.logger.Infof("gc all completed %d records", done)

			i = 0
			addrs = addrs[:0]
		}

		return false, nil
	}, nil)
	if err != nil {
		db.logger.Errorf("set all sync error: %v", err)
	}
	if len(addrs) > 0 {
		err := db.Set(ctx, storage.ModeSetSync, addrs...)
		if err != nil {
			db.logger.Errorf("set all sync error: %v", err)
			return
		}
		done += len(addrs)
	}

	db.logger.Info("set all sync OK, total %d records", done)
}
