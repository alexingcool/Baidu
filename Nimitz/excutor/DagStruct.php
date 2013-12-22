<?php
/**
 * @file DagStruct.php
 * @author zhangliang08(com@baidu.com)
 * @date 2013/12/09 18:11:35
 * @brief 
 *  
 **/

require_once dirname(__FILE__) . '/MetaUtil.php';
 
 class DataNode {
 	public function __construct() {
 		$this->metautil = MetaUtil::getInstance();
 	}
    public function init() {}
 	public $name;
 	public $type;
 	public $isready;
 	public $isreadyexec;
 	public $metautil;
 	public $did;
 }

class Table extends DataNode {
    public function __construct() {
        parent::__construct();
    }
 	public function getTableName() {
 		if (isset($this->tablename)) {
 			return $this->tablename;
 		} else {
 			return $this->metautil->getTableName($this, $this->dbname, $this->tablename, $this->namenode);
 		}
 	}
 	public function getPapiTables() {
 		if (isset($this->papitable)) {
 			return $this->papitable;
 		} else {
            $this->papitable = $this->dbname . "." . $this->tablename;
 			return $this->papitable;
 		}
    }
    public function getDBTable() {
        if (isset($this->papitable)) {
            return $this->papitable;
        } else {
            $this->papitable = $this->dbname . "." . $this->tablename;
            return $this->papitable;
        }
    }
    public function init() {
        self::getTableName();
        self::getDBTable();
        self::getPapiTables();
    }
    public $product;
    public $deptime;
    public $fsname;
    public $partition;
 	public $tablename;
    public $dbname;
    public $papitable;
    public $namenode;
 	public $isstart;
 	public $deptype;
 	public $gentype;
 }
 
 class Log extends DataNode {
 	public function __construct() {
		$this->isstart = true;
        parent::__construct();
 	}
 	public function getLogPath() {
 		if (isset($this->logpath)) {
 			return $this->logpath;
 		} else {
 			return $this->metautil->getLogPathFreq($this, $this->logfreq, $this->logpath, $this->clustername, $this->namenode);
 		}
 	}
    public function init() {
        self::getLogPath();
    }
    public $product;
    public $deptime;
    public $fsname;
    public $partition;
    public $clustername;
 	public $logfreq;
 	public $logpath;
    public $namenode;
 	public $deptype;
 	public $isstart;
 }

 class TempNode extends DataNode {
     public function __construct() {
        parent::__construct();
     }
     public $nodedistinct;
 }

 class DagAttr {
 	public function __construct() {
 		$this->warning_list = array();
 	}
 	public $name;
 	public $version_name;
 	public $start_partition;
 	public $end_partition;
 	public $freq;
 	public $dead_line;
 	public $warning_list;
 	public $etl_framework_version;
 	public $papi_version;
 };
 
 class FlowAttr {
 	public function __construct() {
 		$this->args = array();
 	}
 	public $xml;
 	public $so;
 	public $disp;
 	public $shfile;
 	public $otherfile;
 	public $args;
 }
 
 class FLow {
 	public function __construct() {
 		$this->depindexs = array();
 		$this->ispushed = false;
 	}
 	public $index;
 	public $type;
 	public $startnodes;
 	public $endnodes;
 	public $flowattr;
 	public $isready;
 	public $isreadyexec;
 	public $depindexs;
 	public $fid;
 	public $ispushed;
 }
 
 class Dag {
 	public function __construct() {
 		$this->dagattr = new DagAttr();
 		$this->startnodes = array();
 		$this->endnodes = array();
 		$this->datanodes = array();
 		$this->flows = array();
 		$this->cluster = new Cluster();
 	}
 	
 	public $dagattr;
 	public $startnodes;
 	public $endnodes;
 	public $datanodes;
 	public $flows;
 	public $cluster;
 	
 	const FREQDAY = 1440;
 	const FREQHOUR = 60;
 	const FREQMIN = 15;
 }
 
 class Cluster {
 	public $queue;
 	public $displayer;
 	public $user;
 	public $passwd;
 	public $jobtracker;
 	public $namenode;
 	public $lognamenode;
 }
 

 
