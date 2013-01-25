<?php

class lw_db_transporter
{

    function __construct()
    {
        $this->db = lw_registry::getInstance()->getEntry("db");
        $this->config = lw_registry::getInstance()->getEntry("config");
        $this->setVendorDBTransport();
    }

    function setDebug($bool)
    {
        if ($bool) {
            $this->debug = true;
        }
        else {
            $this->debug = false;
        }
    }

    function setVendorDBTransport()
    {
        if ($this->config['lwdb']['type'] == 'oracle') {
            $this->transport = new lw_db_transport_oracle();
        }
        if ($this->config['lwdb']['type'] == 'mysql' || $this->config['lwdb']['type'] == 'mysqli') {
            $this->transport = new lw_db_transport_mysql();
        }
        $this->transport->setDB($this->db);
        $this->transport->setDebug($this->debug);
    }

    public function getAllTables()
    {
        return $this->transport->getAllTables();
    }

    public function exportData($tables)
    {
        $xml = "<dbdata>\n\n";
        if (is_array($tables)) {
            foreach ($tables as $table => $value) {
                $autoincrement = $this->transport->getAutoincrement($table);

                $xml.='<table name="' . str_replace($this->db->getPrefix(), '', strtolower($table)) . '"';
                if ($autoincrement['value'] > 0) {
                    $xml.=' aifield="' . $autoincrement['field'] . '" aivalue="' . $autoincrement['value'] . '" ';
                }
                $xml.='>' . "\n";
                $data = $this->transport->getAllDataByTable($table);
                foreach ($data as $line) {
                    $xml.='    <entry>' . "\n";
                    $xml.="        <fields>\n";
                    foreach ($line as $key => $value) {
                        if ($value) {
                            if (strval(intval($value)) == strval($value)) {
                                $value = intval($value);
                                $xml.= '            <field name="' . $this->fieldToTag($key) . '" type="int">' . "\n";
                            }
                            else {
                                $xml.= '            <field name="' . $this->fieldToTag($key) . '" type="string">' . "\n";
                                $value = base64_encode($value);
                            }
                            $xml.= '                <![CDATA[' . ($value) . ']]>' . PHP_EOL;
                            $xml.= '            </field>' . "\n";
                        }
                    }
                    $xml.="       </fields>\n";
                    $xml.='    </entry>' . "\n";
                }
                $xml.="</table>\n\n";
            }
        }
        $xml.="</dbdata>\n\n";
        return $xml;
    }

    public function fieldToTag($name)
    {
        $out = str_replace(' ', '_', $name);
        $out = strtolower($out);
        return $out;
    }

    public function exportTables($tables)
    {
        $xml.="<migration>\n";
        $xml.="<version>1</version>\n";
        $xml.="<up>\n";
        if (is_array($tables)) {
            foreach ($tables as $table => $value) {
                if ($value == 1) {
                    $xml.='<createTable name="' . str_replace($this->db->getPrefix(), "", strtolower($table)) . '">' . "\n";
                    $xml.="    <fields>\n";

                    $xml.= $this->_buildTableStructure($table);

                    $pk = $this->transport->getPrimaryKey($table);
                    if (!$pk && $this->forcePK) {
                        $pk = $this->forcePK;
                    }                    
                    if ($pk) {
                        $xml.= '        <pk>' . $pk . '</pk>' . "\n";
                    }
                    $pk = false;

                    $xml.="    </fields>\n";
                    $xml.="</createTable>\n\n";
                }
            }
        }
        $xml.="</up>\n\n";
        $xml.="</migration>\n\n";
        return $xml;
    }

    private function _buildTablestructure($table)
    {
        $columns = $this->transport->getColumnsByTable($table);
        if (is_array($columns)) {
            foreach ($columns as $column) {
                $xml.= '        <field name="' . $this->transport->getColumnName($column) . '"';
                $xml.= $this->transport->parseColumn($column);
                if ($this->transport->hasAutoincrement($table, $column)) {
                    $xml.='special="auto_increment" ';
                    $this->forcePK = $this->transport->getColumnName($column);
                }
                $xml.= '/>' . "\n";
            }
        }
        return $xml;
    }

    public function importXML($xml)
    {
        $xml = trim($xml);
        include_once('FluentDOM/FluentDOM.php');
        $dom = FluentDOM($xml);
        if (substr($xml, 0, strlen('<migration>')) == '<migration>') {
            $ctNodes = $dom->find('/migration/up/createTable');
            foreach ($ctNodes as $ctNode) {
                $ok = $this->transport->createTable($ctNode);
            }
            die("imported!<br/><a href='index.php'>go back</a>");
        }
        elseif (substr($xml, 0, strlen('<dbdata>')) == '<dbdata>') {
            $this->importData($dom);
        }
        die("ung&uuml;ltiges XML!<br/><a href='index.php'>go back</a>");
    }

    function importData($dom)
    {
        $tableNodes = $dom->find('/dbdata/table');
        foreach ($tableNodes as $tableNode) {
            $table = FluentDOM($tableNode);
            $entryNodes = $table->find('entry');
            $sql = "DELETE FROM " . $this->prefix . $table->attr('name');
            if (!$this->debug)
                $ok = $this->db->dbquery($sql);
            echo $ok . ": " . $sql . "<br>";

            foreach ($entryNodes as $entryNode) {
                $clobs = array();
                $id = false;
                $entry = FluentDOM($entryNode);
                $fieldNodes = $entry->find('fields/field');
                foreach ($fieldNodes as $fieldNode) {
                    $field = FluentDOM($fieldNode);
                    if (strlen($field->text()) > 4000) {
                        $clobs[$field->attr('name')] = $this->db->quote(base64_decode($field->text()));
                    }
                    else {
                        if ($field_names) {
                            $field_names.= ", ";
                            $values.= ", ";
                        }
                        $field_names.= $field->attr('name') . " ";
                        if ($field->attr('type') == "string") {
                            $value = $this->db->quote(base64_decode($field->text()));
                        }
                        else {
                            $value = intval($field->text());
                        }
                        $values.= "'" . $value . "' ";
                        if ($field->attr('name') == "id")
                            $id = intval($field->text());
                    }
                }
                $sql = "INSERT INTO " . $this->prefix . $table->attr('name') . " (" . $field_names . ") VALUES (" . $values . ")";
                if (!$this->debug)
                    $ok = $this->db->dbquery($sql);
                echo $ok . ": " . $sql . "<br>";

                foreach ($clobs as $field => $data) {
                    if (!$this->debug)
                        $this->db->saveClob($this->prefix . $table->attr('name'), $field, $data, $id);
                }

                unset($sql);
                unset($field_names);
                unset($values);
                unset($clob);
                unset($id);
            }

            if (strlen(trim($table->attr('aifield'))) > 0) {
                $this->transport->setAutoincrement($this->prefix . $table->attr('name'), ($table->attr('aivalue') + 1));
            }
        }
        die("imported!<br/><a href='index.php'>go back</a>");
    }

}
