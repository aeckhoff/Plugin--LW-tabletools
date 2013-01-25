<?php

class lw_db_transport_oracle implements lw_vendorDBTransport
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
        $result = $this->db->select("select table_name from user_tables ORDER BY table_name ASC");
        foreach($result as $table) {
            $tables[] = $table['table_name'];
        }
        return $tables;
    }

    public function getPrimaryKey($table)
    {
        $sql.= "SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner ";
        $sql.= "FROM all_constraints cons, all_cons_columns cols ";
        $sql.= "WHERE cols.table_name = '".$table."' ";
        $sql.= "AND cons.constraint_type = 'P' ";
        $sql.= "AND cons.constraint_name = cols.constraint_name ";
        $sql.= "AND cons.owner = cols.owner ";
        $sql.= "ORDER BY cols.table_name, cols.position ";
        $pks = $this->db->select($sql);
        if (is_array($pks)) {
            foreach($pks as $pk) {
                if (strlen(trim($primarykey))>0) {
                    $primarykey.= ", ";
                }
                $primarykey.= strtolower($pk['column_name']);
            }
            return $primarykey;
        }
        return false;    
    }
    
    public function getColumnsByTable($table)
    {
        $sql = "select table_name, column_name, data_type, data_length, data_precision, nullable, column_id from user_tab_columns where table_name = '".$table."' order by column_id ASC";
        return $this->db->select($sql);
    }
    
    public function getColumnName($column)
    {
        return strtolower($column['column_name']);
    }
    
    public function parseColumn($column)
    {
		if ($column['data_type'] == "NUMBER") {
			return ' type="number" size="'.$column['data_precision'].'" ';
		}
		elseif($column['data_type'] == "CLOB") {
			return ' type="clob" ';
		}
		else {
			return ' type="text" size="'.$column['data_length'].'" ';
		}    
    }
    
    public function hasAutoincrement($table, $column)
    {
        if ($column['column_name'] == "ID") {
            $sql = "select trigger_name from user_triggers WHERE table_name = '".$table."' ";
            $trigger = $this->db->select1($sql);
            if (substr($trigger['trigger_name'], -3) == "_IB") {
                return true;
            }
        }
        return false;
    }
    
    function getAllIndexes($table) 
    {
        return false;
        
        /* 
        ** noch besser ausarbeiten!!!!!
        **
        
        $sql = "select con.constraint_type, col.column_name from user_constraints con, user_ind_columns col WHERE col.index_name = con.index_name AND con.table_name ='".$table."'";
        $indexes = $this->db->select($sql);
        if (is_array($indexes)) {
            foreach($indexes as $id) {
                if (strlen(trim($index))>0) {
                    $index.= ", ";    
                }
                $index.= strtolower($pk['column_name']);
            }
            return $primarykey;
        }
        return false; 
        */       
    }
    
    function getAutoincrement($table) 
    {
        $columns = $this->getColumnsByTable($table);
        if (is_array($columns)) {
            foreach($columns as $column) {
                if ($column['column_name'] == "ID") {
                    $sql = "select trigger_name from user_triggers WHERE table_name = '".$table."' ";
                    $trigger = $this->db->select1($sql);
                    if (substr($trigger['trigger_name'], -3) == "_IB") {
                        $autoincrement['field'] = "id";
                        $value = $this->db->select1("SELECT max(id) as maxauto FROM ".$table);
                        $autoincrement['value'] = $value['maxauto']+1;
                        return $autoincrement;
                    }
                }
            }
        }
        return false;        
    }
    
    function getAllDataByTable($table) 
    {
        return $this->db->select("SELECT * FROM ".$table);					
    }    
    
    function createTable($ctNode)
	{
		$ct 		= FluentDOM($ctNode);
		$head 		= "CREATE TABLE ".$this->prefix.$ct->attr('name')." (\n";
        $this->ai 	= false;
		
		foreach($ct->find('fields/field') as $fieldnode)
		{
			$field = FluentDOM($fieldnode);
			if (strlen($main)>0)
			{
			    $main.=",\n";
			}
			$main.= $this->_buildField($field);
		}
		$main.= ')'."\n";
		echo '<pre>'.nl2br($head.$main).'</pre>';
		if($this->debug != 1)
		{
			$this->db->dbquery($head.$main);
		}
		
		$pk = $ct->find('fields/pk')->text();		
		if (strlen($pk)>0) $this->_addPK($this->prefix.$ct->attr('name'), $pk);
		if ($this->ai == true) $this->_addAutoIncrement($this->prefix.$ct->attr('name'));	
	}
	
	private function _buildField($field)
	{
		$out.= '    '.strtoupper($field->attr('name'));
		switch($field->attr('type'))
		{
			case "number":
				$out.=" NUMBER(".$field->attr('size').") ";
				break; 

			case "text":
				$out.= " VARCHAR2(".$field->attr('size').") ";
				break; 

			case "clob":
				$out.= " CLOB ";
				break; 
				
			case "bool":
				$out.= " NUMBER(1) ";
				break; 
				
			default:
				die("field not available");
		}
		if ($field->attr('special') == 'auto_increment')
		{
			$this->ai = true;
		} 
		return $out;	
	}
	
	private function _addPK($table, $pk) 
	{
		$sql = "ALTER TABLE ".$table." ADD ( PRIMARY KEY ( ".$pk." ) )";
		if ($this->debug) { 
		    echo "<p><pre>".$sql."</pre></p>\n"; 
		} 
		else { 
		    $ok = $this->db->dbquery($sql); 
		}
	}
	
	private function _addAutoIncrement($table)
	{
		$sql = "DROP SEQUENCE ".$table."_SEQ";
		if ($this->debug) { 
		    echo "<p><pre>".$sql."</pre></p>\n"; 
		}
		else { 
		    $ok = $this->db->dbquery($sql); 
        }
		$sql = "CREATE SEQUENCE ".$table."_SEQ START WITH 1 INCREMENT BY 1 MAXVALUE 1E27 MINVALUE 1 NOCACHE NOCYCLE ORDER";
		if ($this->debug) { 
		    echo "<p><pre>".$sql."</pre></p>\n"; 
		} 
		else { 
		    $ok = $this->db->dbquery($sql); 
		}
		$sql = "CREATE OR REPLACE TRIGGER ".$table."_ib BEFORE INSERT ON ".$table." FOR EACH ROW  BEGIN IF :new.id IS null THEN select ".$table."_SEQ.nextval into :new.id from dual; END IF; END;";
		if ($this->debug) { 
    		echo "<p><pre>".$sql."</pre></p>\n"; 
		} 
		else { 
    		$ok = $this->db->dbquery($sql); 
		}	
	}
	
	public function setAutoincrement($table, $value) 
	{
	    $sql = "ALTER SEQUENCE ".$$table."_SEQ INCREMENT BY ".$value;
	    if (!$this->debug) $ok = $this->db->dbquery($sql);
	    echo $ok.": ".$sql."<br>";
	}	
}
