<?php

class lw_db_transport_mysql implements lw_vendorDBTransport
{

    public function __construct()
    {
        
    }

    public function setDebug($debug)
    {
        $this->debug = $debug;
    }

    public function setDB(lw_db $db)
    {
        $this->db = $db;
    }

    public function getAllTables()
    {
        $result = $this->db->select("show tables");
        foreach ($result as $single) {
            foreach ($single as $key => $table) {
                $tables[] = $table;
            }
        }
        return $tables;
    }

    public function getPrimaryKey($table)
    {
        $columns = $this->getColumnsByTable($table);
        foreach ($columns as $column) {
            if ($column['Key'] == "PRI") {
                if (strlen(trim($pk)) > 0)
                    $pk.=',';
                $pk.= $column['Field'];
            }
        }
        return $pk;
    }

    public function getColumnsByTable($table)
    {
        return $this->db->getTableStructure($table);
    }

    public function getColumnName($column)
    {
        return $column['Field'];
    }

    public function parseColumn($column)
    {
        $value = $column['Type'];
        $parts = explode('(', $value);
        $mtype = $parts[0];
        $msize = intval(str_replace(')', '', $parts[1]));

        if (in_array($mtype, array('int', 'bigint', 'tinyint'))) {
            $type = 'type="number"';
        }
        elseif (in_array($mtype, array('longtext')) || in_array($mtype, array('mediumtext'))) {
            $type = 'type="clob"';
        }
        else {
            $type = 'type="text"';
        }
        if ($msize > 0) {
            $size = 'size="' . $msize . '"';
        }
        elseif ($mtype == 'text') {
            $size = 'size="3999"';
        }
        return ' ' . $type . ' ' . $size . ' ';
    }

    public function hasAutoincrement($table, $column)
    {
        if ($column['Extra'] == "auto_increment") {
            return true;
        }
        return false;
    }

    function getAllIndexes($table)
    {
        return false;
    }

    function getAutoincrement($table)
    {
        $columns = $this->getColumnsByTable($table);
        foreach ($columns as $column) {
            if ($column['Extra'] == "auto_increment") {
                $autoincrement['field'] = $column['Field'];
                $data = $this->db->select1("SELECT max(" . $autoincrement['field'] . ") as maxauto FROM " . $table);
                $autoincrement['value'] = intval($data['maxauto']);
            }
        }
        return $autoincrement;
    }

    function getAllDataByTable($table)
    {
        return $this->db->select("SELECT * FROM " . $table);
    }

    function createTable($ctNode)
    {
        $ct = FluentDOM($ctNode);
        $head = "CREATE TABLE IF NOT EXISTS " . $this->db->getPrefix() . $ct->attr('name') . " (\n";

        foreach ($ct->find('fields/field') as $fieldnode) {
            $field = FluentDOM($fieldnode);
            $main.= $this->_buildField($field);
        }

        $pk = $ct->find('fields/pk')->text();
        if (strlen($pk) > 0) {
            $foot = "	PRIMARY KEY (" . $pk . ")\n";
        }
        else {
            $main = substr($main, 0, -2) . "\n";
        }
        $foot.= ")\n";
        if ($this->debug != 1) {
            $this->db->dbquery($head . $main . $foot);
        }
        echo '<pre>' . nl2br($head . $main . $foot) . '</pre>';
    }

    private function _buildField($field)
    {
        $out.= '    ' . $field->attr('name');
        switch ($field->attr('type')) {
            case "number":
                if ($field->attr('size') > 11) {
                    $out.=" bigint(" . $field->attr('size') . ") ";
                }
                else {
                    $out.= " int(" . $field->attr('size') . ") ";
                }
                break;

            case "text":
                if ($field->attr('size') > 255) {
                    $out.= " text ";
                }
                else {
                    $out.= " varchar(" . $field->attr('size') . ") ";
                }
                break;

            case "clob":
                $out.= " longtext ";
                break;

            case "bool":
                $out.= " int(1) ";
                break;

            default:
                die("field not available");
        }
        if ($field->attr('special') == 'auto_increment') {
            $out.=' auto_increment ';
        }
        return $out . ",\n";
    }

    public function setAutoincrement($table, $value)
    {
        $sql = "ALTER TABLE " . $table . " AUTO_INCREMENT = " . $value;
        if (!$this->debug)
            $ok = $this->db->dbquery($sql);
        echo $ok . ": " . $sql . "<br>";
    }

}
