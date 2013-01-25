<?php

interface lw_vendorDBTransport
{
    public function setDB(lw_db $db);
    public function getPrimaryKey($table);
    public function getColumnsByTable($table);
    public function getColumnName($column);
    public function parseColumn($column);
    public function hasAutoincrement($table, $column);
}
