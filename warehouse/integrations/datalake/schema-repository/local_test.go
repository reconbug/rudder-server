package schemarepository_test

import (
	"fmt"
	"testing"
	"time"

	schemarepository "github.com/rudderlabs/rudder-server/warehouse/integrations/datalake/schema-repository"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

type mockUploader struct {
	mockError   error
	localSchema warehouseutils.Schema
}

func (*mockUploader) GetSchemaInWarehouse() warehouseutils.Schema              { return warehouseutils.Schema{} }
func (*mockUploader) ShouldOnDedupUseNewRecord() bool                          { return false }
func (*mockUploader) UseRudderStorage() bool                                   { return false }
func (*mockUploader) GetLoadFileGenStartTIme() time.Time                       { return time.Time{} }
func (*mockUploader) GetLoadFileType() string                                  { return "JSON" }
func (*mockUploader) GetFirstLastEvent() (time.Time, time.Time)                { return time.Time{}, time.Time{} }
func (*mockUploader) GetTableSchemaInUpload(string) warehouseutils.TableSchema { return nil }
func (*mockUploader) GetSampleLoadFileLocation(string) (string, error)         { return "", nil }
func (*mockUploader) GetLoadFilesMetadata(warehouseutils.GetLoadFilesOptions) []warehouseutils.LoadFile {
	return nil
}

func (*mockUploader) GetTableSchemaInWarehouse(string) warehouseutils.TableSchema {
	return warehouseutils.TableSchema{}
}

func (*mockUploader) GetSingleLoadFile(string) (warehouseutils.LoadFile, error) {
	return warehouseutils.LoadFile{}, nil
}

func (m *mockUploader) GetLocalSchema() (warehouseutils.Schema, error) {
	return m.localSchema, nil
}

func (m *mockUploader) UpdateLocalSchema(warehouseutils.Schema) error {
	return m.mockError
}

func TestLocalSchemaRepository_CreateTable(t *testing.T) {
	testCases := []struct {
		name        string
		mockError   error
		localSchema warehouseutils.Schema
		wantError   error
	}{
		{
			name:        "success",
			localSchema: warehouseutils.Schema{},
		},
		{
			name: "table already exists",
			localSchema: warehouseutils.Schema{
				"test_table": {
					"test_column_1": "test_type_1",
				},
			},
			wantError: fmt.Errorf("failed to create table: table %s already exists", "test_table"),
		},
		{
			name:        "error updating local schema",
			mockError:   fmt.Errorf("error updating local schema"),
			wantError:   fmt.Errorf("error updating local schema"),
			localSchema: warehouseutils.Schema{},
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			warehouse := warehouseutils.Warehouse{}
			uploader := &mockUploader{
				mockError:   tc.mockError,
				localSchema: tc.localSchema,
			}

			s, err := schemarepository.NewLocalSchemaRepository(warehouse, uploader)
			require.NoError(t, err)

			err = s.CreateTable("test_table", warehouseutils.TableSchema{
				"test_column_2": "test_type_2",
			})
			if tc.wantError != nil {
				require.EqualError(t, err, tc.wantError.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestLocalSchemaRepository_AddColumns(t *testing.T) {
	testCases := []struct {
		name        string
		mockError   error
		localSchema warehouseutils.Schema
		wantError   error
	}{
		{
			name: "success",
			localSchema: warehouseutils.Schema{
				"test_table": {
					"test_column_1": "test_type_1",
				},
			},
		},
		{
			name:      "table does not exists",
			wantError: fmt.Errorf("failed to add column: table %s does not exist", "test_table"),
		},
		{
			name: "error updating local schema",
			localSchema: warehouseutils.Schema{
				"test_table": {
					"test_column_1": "test_type_1",
				},
			},
			mockError: fmt.Errorf("error updating local schema"),
			wantError: fmt.Errorf("error updating local schema"),
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			warehouse := warehouseutils.Warehouse{}
			uploader := &mockUploader{
				mockError:   tc.mockError,
				localSchema: tc.localSchema,
			}

			s, err := schemarepository.NewLocalSchemaRepository(warehouse, uploader)
			require.NoError(t, err)

			err = s.AddColumns("test_table", []warehouseutils.ColumnInfo{
				{
					Name: "test_column_2",
					Type: "test_type_2",
				},
			})
			if tc.wantError != nil {
				require.EqualError(t, err, tc.wantError.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestLocalSchemaRepository_AlterColumn(t *testing.T) {
	testCases := []struct {
		name        string
		mockError   error
		localSchema warehouseutils.Schema
		wantError   error
	}{
		{
			name: "success",
			localSchema: warehouseutils.Schema{
				"test_table": {
					"test_column_1": "test_type_1",
				},
			},
		},
		{
			name:      "table does not exists",
			wantError: fmt.Errorf("failed to add column: table %s does not exist", "test_table"),
		},
		{
			name: "column does not exists",
			localSchema: warehouseutils.Schema{
				"test_table": {
					"test_column_2": "test_type_2",
				},
			},
			wantError: fmt.Errorf("failed to alter column: column %s does not exist in table %s", "test_column_1", "test_table"),
		},
		{
			name: "error updating local schema",
			localSchema: warehouseutils.Schema{
				"test_table": {
					"test_column_1": "test_type_1",
				},
			},
			mockError: fmt.Errorf("error updating local schema"),
			wantError: fmt.Errorf("error updating local schema"),
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			warehouse := warehouseutils.Warehouse{}
			uploader := &mockUploader{
				mockError:   tc.mockError,
				localSchema: tc.localSchema,
			}

			s, err := schemarepository.NewLocalSchemaRepository(warehouse, uploader)
			require.NoError(t, err)

			_, err = s.AlterColumn("test_table", "test_column_1", "test_type_2")
			if tc.wantError != nil {
				require.EqualError(t, err, tc.wantError.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}
