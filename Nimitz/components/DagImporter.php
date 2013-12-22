<?php
/**
 * @file DagImporter.php
 * @author zhangliang(com@baidu.com)
 *date 2013/11/19 10:18:21
 * @brief
 *
 **/

require_once dirname(__FILE__)."/Exception.php";
require_once dirname(__FILE__)."/EtlUtil.php";
require_once dirname(__FILE__).'/EtlVersionTool.php';
require_once dirname(__FILE__).'/PackageTool.php';
require_once dirname(__FILE__).'/ClusterTools.php';
require_once dirname(__FILE__).'/FileManagerTool.php';
require_once dirname(__FILE__).'/FlowTool.php';
require_once dirname(__FILE__).'/ETLTools.php';
require_once dirname(__FILE__).'/DQImporter.php';
require_once dirname(dirname(__FILE__)).'/models/DBEtl.php';
require_once dirname(dirname(__FILE__)).'/models/DBEtlVersion.php';
require_once dirname(dirname(__FILE__)).'/models/DBSo.php';
require_once dirname(dirname(__FILE__)).'/models/DBSoUsage.php';
require_once dirname(dirname(__FILE__)).'/models/DBEtlJobTracker.php';
require_once dirname(dirname(__FILE__)).'/models/DBEtlHdfs.php';
require_once dirname(dirname(__FILE__)).'/models/DBDispatching.php';
require_once dirname(dirname(__FILE__)).'/models/DBDispatchCond.php';
require_once dirname(dirname(__FILE__)).'/models/DBGen.php';
require_once dirname(dirname(__FILE__)).'/models/DBDep.php';
require_once dirname(dirname(__FILE__)).'/models/DBMappingType.php';
require_once dirname(dirname(__FILE__)).'/models/DBFlow.php';
require_once dirname(dirname(__FILE__)).'/models/DBEtlConcerner.php';
require_once dirname(dirname(__FILE__)).'/models/DBDatanode.php';
require_once dirname(dirname(__FILE__)).'/models/DBDataQualitSet.php';
require_once dirname(dirname(__FILE__)).'/models/DBEtlNamespace.php';
require_once dirname(dirname(__FILE__)).'/models/DBFieldMapping.php';
require_once dirname(dirname(__FILE__)).'/models/DBDataQualitSet.php';

interface ImportNodes {
    public function Import($info);
}

class Node {
    public function GetNodes($inodes) {
        if (!is_array($inodes)) {
            throw new ImporterException(__FILE__, __FUNCTION__, __LINE__, "Interface DataNodes: Function Input Must be Array");
        }
        $nodes = array();
        foreach ($inodes as $inode) {
            $nodes[] = $inode['name'];
        }
        return $nodes;
    }
}

class ImportStartNodes extends Node implements ImportNodes {
    public function Import($dataFlow) {
        $dagStartNodes = array();
        foreach ($dataFlow->StartNodes as $startNodes) {
            foreach ($startNodes as $startNode) {
                $name = "";
                $arr = array();
                foreach ($startNode->attributes() as $key => $value) {
                    if ($key === "name") {
                        $name = (string)$value;
                    }
                    $arr[$key] = (string)$value;
                }
                $dagStartNodes[$name] = $arr;
            }
        }
        return $dagStartNodes;
    }
}

class ImportEndNodes extends Node implements ImportNodes {
    public function Import($dataFlow) {
        $dagEndNodes = array();
        foreach ($dataFlow->EndNodes as $endNodes) {
            foreach ($endNodes as $endNode) {
                $name = "";
                $arr = array();
                foreach ($endNode->attributes() as $key => $value) {
                    if ($key === "name") {
                        $name = (string)$value;
                    }
                    $arr[$key] = (string)$value;
                }
                $dagEndNodes[$name] = $arr;
            }
        }
        return $dagEndNodes;
    }
}

abstract class Importer {
    function __construct($fileName, $directory) {
        $this->_file = $fileName;
        $file = $directory."/".$fileName;
        $this->_xml = simplexml_load_file($file);
    }

    abstract function ImportXml();
    abstract function XmlToSql();

    function GetXml() {
        return $this->_xml;
    }
    function GetFile() {
        return $this->_file;
    }

    private $_xml;
    private $_dir;
    private $_file;
};

class DagImporter extends Importer {
    function __construct($fileName, $directorys, $nid) {
        $dir = $directorys['dag'];
        $this->_sodir = $directorys['so'];
        $this->_filedir = $directorys['file'];
        $this->_shdir = $directorys['shell'];
        $this->_dqdir = $directorys['dq'];
        $this->_flowid = array();
        $this->_soid = array();
        $this->_fileid = array();
        $this->_sfileid = array();
        $this->_idnode = array();
        $this->_startnodes = array();
        $this->_endnodes = array();
        $this->_uploadso = array();
        $this->_dispflow = array();
        $this->_nodeid = array();
        $this->_flowid2flow = array();
        parent::__construct($fileName, $dir);
        $this->_dag['dag']['name'] = "";
        $this->_args['nid'] = $nid;
        $this->_dag['dag']['freq'] = 60;
        $this->_dag['dag']['start_partition'] = '201207010000';
        $this->_dag['dag']['end_partition'] = '202307010000';
        $this->_args['auth_user'] = "";
        $this->_args['op_user'] = "";
        $this->_args['comment'] = "";
        $this->_args['jtname'] = 'SZWG-ECOM';
        $this->_args['fsname'] = 'SZWG-ECOM';
        $this->_args['deptype'] = 0;
        $this->_args['gentype'] = 0;
        $this->_args['name'] = "";
        $this->_args['creator'] = "";
        $this->_args['status'] = 0;
    }

    public function GetEvid() {
        return $this->_evid;
    }
    public function GetSoid() {
        return $this->_soid;
    }
    public function GetFileid() {
        return $this->_fileid;
    }
    public function GetSfileid() {
        return $this->_sfileid;
    }
    public function GetFlowId() {
        return $this->_flowid;
    }
    public function GetNodeId() {
        return $this->_nodeid;
    }
    public function GetDispFlow() {
        return $this->_dispflow;
    }
    public function GetStartNode2FlowId() {
        return $this->_startnode2flowid;
    }

    public function __get($property) {
        $method = "Get{$property}";
        if (method_exists($this, $property)) {
            return $this->$method();
        }
    }

    function ImportXml() {
        $xml = parent::GetXml();
        foreach ($xml->DAG as $dag) {
            $dagAttr = array();
            $dagDataNodes = array();
            $dagDataFlows = array();

            foreach($dag->attributes() as $key => $value) {
                $dagAttr[$key] = (string)$value;
            }

            $dagDataNodes = self::_ImportDataNodes($dag);
            $dagDataFlows = self::_ImportDataFlows($dag);

            $this->_dag["dag"] = $dagAttr;
            $this->_dag["datanodes"] = $dagDataNodes;
            $this->_dag["dataflows"] = $dagDataFlows;
        }
        return $this->_dag;
    }

    function XmlToSql() {
        $eid = self::_XmlToEtl();
        $evid = self::_XmlToEtlVersion($eid);
        self::_XmlToDAG($evid);
    }

    private function _ImportDataNodes($Dag) {
        $dagDataNodes = array();
        foreach ($Dag->DataNodes as $dagNodes) {  
            foreach ($dagNodes as $dagNode) {
                $arr = array();
                foreach ($dagNode->attributes() as $key => $value) {
                    $arr[$key] = (string)$value;
                }
                $dagDataNodes[] = $arr;
            }
        }
        return $dagDataNodes;
    }

    private function _ImportDataFlows($Dag) {
        $dagDataFlows = array();
        $dataFlowIndex = 0;
        $dataIndex = 0;
        foreach ($Dag->DataFlows as $dataFlows) {
            foreach ($dataFlows as $dataFlow) {
                $dagDataFlow = array();
                $dagStartNodes = array();
                $dagEndNodes = array();
                $dagFunction = array();
                $flowAttr = array();

                $importNodes = new ImportStartNodes();
                $dagStartNodes = $importNodes->Import($dataFlow);
                $importNodes = new ImportEndNodes();
                $dagEndNodes = $importNodes->Import($dataFlow);

                foreach ($dataFlow->attributes() as $key => $value) {
                    $flowAttr[$key] = (string)$value;
                }

                foreach ($dataFlow->Function->attributes() as $key => $value) {
                    $dagFunction[$key] = (string)$value;
                }
                $dagDataFlow["startnodes"] = $dagStartNodes;
                $dagDataFlow["endnodes"] = $dagEndNodes;
                $dagDataFlow["function"] = $dagFunction;
                $dagDataFlow["attribute"] = $flowAttr;
                $dagDataFlows[$dataFlowIndex++] = $dagDataFlow;  
            }
        }
        return $dagDataFlows;
    }

    private function _XmlToEtl() {
        $name = $this->_dag['dag']['name'];
        $nid = $this->_args['nid'];
        $interval = (int)$this->_dag['dag']['freq'];
        $interval *= 60;
        $start_time = strtotime($this->_dag['dag']['start_partition']);
        $end_time = strtotime($this->_dag['dag']['end_partition']);
        $auth_user = $this->_args['auth_user'];
        $op_user = $this->_args['op_user'];
        $comment = $this->_args['comment'];
        $jtname = $this->_dag['dag']['cluster'];
        $fsname = "";
        if (array_key_exists('fsname', $this->_dag['dag'])) {
            $fsname = $this->_dag['dag']['fsname'];
        }

        $deptype = (int)$this->_args['deptype'];
        $gentype = (int)$this->_args['gentype'];
        $status = 0;
        $jobid = 0;

        $fsid = 1;
        $jtid = 1;
        $ret = self::_GetIdByJtFsName($jtname, $fsname);
        if (array_key_exists('fsid', $ret)) {
            $fsid = $ret['fsid'];
        }
        if (array_key_exists('jtid', $ret)) {
            $jtid = $ret['jtid'];
        }
        $instance = ETLTools::getInstance();
        $etl = null;
        $ret = $instance->IsNameIn($name, $etl);
        if ($ret === false) {
            $etl = DBEtl::insertEtl($name, $nid, $interval, $start_time, $end_time, $auth_user, $op_user,
                $comment, $status, $jtid, $fsid, $deptype, $gentype, $jobid);
            if ($etl == null) {
                throw new ImporterException(__FILE__, __FUNCTION__, __LINE__, "Insert ETL Failed");
            } 
        }

        $eid = $etl->eid();
        $warning_list = $this->_dag['dag']['warning_list'];
        $concerners = explode(',', $warning_list);
        foreach ($concerners as $concerner) {
            $etlversion = DBEtlConcerner::insertConcerner($eid, $concerner, $comment);
        }

        return $etl->eid();
    } 

    private function _GetIdByJtFsName($jtname, $fsname) {
        $cluster = ClusterTools::getInstance();
        $clusterinfo = $cluster->getClusters();
        if ($clusterinfo['status'] === 'failure') {
            throw new ImporterException(__FILE__, __FUNCTION__, __LINE__, "Get Cluster Info Failed");
        }
        $hdfses = $clusterinfo['hdfses'];
        $jobtrackers = $clusterinfo['jobTrackers'];
        $ret = array();
        foreach ($hdfses as $hdfs) {
            if ($hdfs->name() === $fsname) {
                $ret['fsid'] = $hdfs->fsid();
                break;
            }
        }
        foreach ($jobtrackers as $jobtracker) {
            if ($jobtracker->name() === $jtname) {
                $ret['jtid'] = $jobtracker->jtid();
                break;
            }
        }
        return $ret;
    }

    private function _FillStartNodeInfo($dataFlow, $flowid) {
        $startnodes = $dataFlow['startnodes'];
        $endnodes = $dataFlow['endnodes'];
        foreach ($startnodes as $startnode) {
            if (in_array($startnode['name'], $this->_startnodes)) {
                $this->_startnode2flowid[$startnode['name']] = $flowid;
            }
        }
        foreach ($endnodes as $endnode) {
            if (in_array($endnode['name'], $this->_endnodes)) {
                $this->_endnode2flowid[$endnode['name']] = $flowid;
            }
        }
    }

    private function _XmlToDAG($evid) {
        $dataFlows = $this->_dag['dataflows'];
        $soarr = array();
        $dids = null;
        $dids = self::_XmlToDataNode($evid);
        //self::_GetStartEndNodes();
        foreach ($dataFlows as $dataFlow) {
            $function = $dataFlow['function'];
            $soid = self::_XmlToSo($evid, $dataFlow);
            $fileid = self::_XmlToFile($evid, $dataFlow);
            $sfileid = self::_XmlToShell($evid, $dataFlow, $shname);
            $flowid = self::_XmlToFlow($evid, $dataFlow, $soid, $fileid, $sfileid);
            $this->_flowid[] = $flowid;
            $this->_soid[$flowid] = $soid;
            $this->_fileid[$flowid] = $fileid;
            $this->_sfileid[$flowid] = $sfileid;
            self::_FillStartNodeInfo($dataFlow, $flowid);

            if (array_key_exists('dispacher', $function)) {
                $disp = $function['dispacher'];
                $dispname = GetLastName($disp);
                $this->_dispflow[$dispname] = $flowid;
            }
        }
        self::_XmlToDepGen($dids);
        DQImporter::import((integer)$evid, $this->_dqdir);
    }

    private function _XmlToSo($evid, $dataFlow) {
        $function = $dataFlow['function'];
        if (!array_key_exists('so', $function)) {
            return null;
        }
        $so = $function['so'];
        $soname = GetLastName($so);
        if (array_key_exists($soname, $this->_uploadso)) {
            return $this->_uploadso[$soname];
        }
        $sofile = $this->_sodir."/".$soname;
        $comment = "";
        if (!file_exists($sofile)) {
            throw new ImporterException(__FILE__, __FUNCTION__, __LINE__, "So File Not exist");
        }
        $usage = FileManagerTool::uploadSo($evid, $sofile, $comment);
        $soid = $usage->soid();
        $this->_uploadso[$soname] = $soid;
        return $soid;
    }

    private function _XmlToFile($evid, $dataFlow) {
        $fileid = null;
        $function = $dataFlow['function'];
        if (!array_key_exists('other_file', $function)) {
            return $fileid;
        }
        $file = $function['other_file'];
        $filename = GetLastName($file);
        $filepath = $this->_filedir."/".$filename;
        $comment = "";
        if (!file_exists($filepath)) {
            throw ImporterException(__FILE__, __FUNCTION__, __LINE__, "Other File Not exist");
        }
        try {
            $fileUsage = FileManagerTool::uploadFile($evid, $filepath, $comment);
            return $fileUsage->fileid();
        } catch (Exception $ex) {
            $exi = $ex->getMessage();
            $info = "$exi: evid = $evid, filepath = $filepath, comment = $comment";
            throw new ImporterException(__FILE__, __FUNCTION__, __LINE__, $info);
        }
    }

    private function _XmlToShell($evid, $dataFlow, &$shname) {
        $sfileid = null;
        do {
            $function = $dataFlow['function'];
            if (!array_key_exists('shell', $function)) {
                break;
            }
            $path = $function['shell'];
            $shname = GetLastName($path);
            $shfile = $this->_shdir."/".$shname;
            $comment = "";
            if (!file_exists($shfile)) {
                throw ImporterException(__FILE__, __FUNCTION__, __LINE__, "Shell File Not exist");
            }
            try {
                $fileUsage = FileManagerTool::uploadFile($evid, $shfile, $comment);
                $sfileid = $fileUsage->fileid();
            } catch (Exception $ex) {
                $exi = $ex->getMessage();
                $info = "$exi: evid = $evid, shfile = $shfile, comment = $comment";
                throw new ImporterException(__FILE__, __FUNCTION__, __LINE__, $info);
            }
        } while (0);
        return $sfileid;
    }

    private function _SetSelfParttern($datanode) {
        $freq = (int)$this->_dag['dag']['freq'];
        $part = null;
        $parts = array();
        $subparts = array();
        $pattern = array();
        if (array_key_exists('partition', $datanode)) {
            $part = $datanode['partition'];
        }
        if (array_key_exists('deptime', $datanode)) {
            $deptime = $datanode['deptime'];
            $matches = null;
            $ret = preg_match('{-?\d+, ?\d+}', $deptime, $matches);
            $times = explode(',', $matches[0]);
            $basetime = $times[0];
            $lasttime = $times[1];
        } 

        if ($freq < 15) {
            $parts = explode(';', $part);
            foreach ($parts as $subpart) {
                $arr = array();
                $arr['event_day'] = '{{day($base)}}';
                $arr['event_hour'] = '{{hour($base)}}';
                $arr['event_min'] = '{{min($base)}}';
                if (array_key_exists('partition', $datanode)) {
                    $part = $datanode['partition'];
                    $subparts = explode('.', $subpart);
                    foreach ($subparts as $subpart) {
                        $keyv = explode('=', $subpart);
                        $arr[$keyv[0]] = $keyv[1];
                    }
                }
                $pattern[] = $arr;
            }
        } else if ($freq == 60) {
            $parts = explode(';', $part);
            foreach ($parts as $subpart) {
                $arr = array();
                $arr['event_day'] = '{{day($base)}}';
                $arr['event_hour'] = '{{hour($base)}}';
                if (array_key_exists('partition', $datanode)) {
                    $part = $datanode['partition'];
                    $subparts = explode('.', $subpart);
                    foreach ($subparts as $subpart) {
                        $keyv = explode('=', $subpart);
                        $arr[$keyv[0]] = $keyv[1];
                    }
                }
                $pattern[] = $arr;
            }
        } else if ($freq === 1440) {
            $parts = explode(';', $part);
            foreach ($parts as $subpart) {
                $arr = array();
                $arr['event_day'] = '{{day($base)}}';
                if (array_key_exists('partition', $datanode)) {
                    $part = $datanode['partition'];
                    $subparts = explode('.', $subpart);
                    foreach ($subparts as $subpart) {
                        $keyv = explode('=', $subpart);
                        $arr[$keyv[0]] = $keyv[1];
                    }
                }
                $pattern[] = $arr;
            }
        }
        return $pattern;
    }

    private function _XmlToDataNode($evid) {
        $dids = array();
        $datanodes = $this->_dag['datanodes'];
        foreach ($datanodes as $key => $datanode) {
            $name = $datanode['name'];
            $typeis = $datanode['type'];
            $type = null;
            if (!is_integer($typeis)) {
                $type = DBDatanode::typeStrToInt($typeis);
            } else {
                $type = (int)$typeis;
            }
            $path = "";
            if ($typeis === 'event') {
                $path = "default.".$name;
            } else if ($typeis === 'big_event') {
                $path = "udw.udw_event".$name;
            } else if ($typeis === 'temp') {
                if (array_key_exists('hdfs_path', $datanode)) {
                    $path = $datanode['hdfs_path'];
                } else {
                    $path = "";
                }
            } else {
                $path = $datanode['product'].".".$name;
            }
            $depPattern = null;
            if (array_key_exists('deptime', $datanode)) {
                $depPattern['deptime'] = $datanode['deptime'];
            } else {
                $depPattern['deptime'] = "{0, ".$this->_dag['dag']['freq']."}";
            }
            $selfDepPattern = null;
            $selfIdxPattern = null;
            $selfPartPattern = null;
            $deptype = 1;
            $gentype = 1;

            if (array_key_exists('self_index_pattern', $datanode)) {
                $selfIdxPattern = $datanode['self_index_pattern'];
            }
            /*
            //$selfDepPattern[] = self::_SetSelfParttern($datanode);
            if (array_key_exists('partition', $datanode)) {
                $selfPartPattern['partition'] = $datanode['partition'];
            }
             */
            $selfPartPattern = self::_SetSelfParttern($datanode);
            
            //currently x & y equals zero, should be caculate a value
            $x = 0;
            $y = 0;
            if (array_key_exists('x', $datanode)) {
                $x = (int)$datanode['x'];
            }
            if (array_key_exists('y', $datanode)) {
                $y = (int)$datanode['y'];
            }

            $fsid = null;
            if (array_key_exists('fsname', $datanode)) {
                $ret = self::_GetIdByJtFsName("", $datanode['fsname']);
                $fsid = $ret['fsid'];
            }

            //dq related 
            $sysDqSetid = null;
            if (array_key_exists('sysDqSetName', $datanode)) {
                $sysDqSetName=$datanode['sysDqSetName'];
                $defaultDQSet=DBDefaultDQSet::getByName($sysDqSetName);
                if(!($defaultDQSet instanceof DBDefaultDQSet)){
                    throw new Exception("Get DefaultDQSet name=$sysDqSetName fail!");
                }
                $sysDqSetid =$defaultDQSet->setid();
            }
            $dataQualitSet=DBDataQualitSet::insertDataQualitSet(DBDataQualitSet::TYPE_VERSION);
            if(!($dataQualitSet  instanceof DBDataQualitSet) ){
                throw new Exception("Insert new DataQualitSet fail!");
            }
            $localDqSetid = $dataQualitSet->setid();
            $conf = "";

            try {
                $datanode = EtlVersionTool::createDatanode($evid, $type, $name, $x, $y, $path, $depPattern, $selfDepPattern,
                    $selfIdxPattern, $selfPartPattern, $sysDqSetid, $localDqSetid, $fsid, $deptype, $gentype, $conf);
            } catch (Exception $ex) {
                throw new ImporterException(__FILE__, __FUNCTION__, __LINE__, $ex->getMessage());
            }
            $did = $datanode->did();
            $dids[] = $did;
            $this->_idnode[$did] = $name;
            $this->_nodeid[$name] = $did;
        }
        return $dids; 
    }

    private function _GetStartEndNodes() {
        $datanodes = $this->_dag['datanodes'];
        foreach ($datanodes as $datanode) {
            if ($datanode['type'] != 'event' && $datanode['type'] != 'big_event' && $datanode['type'] != 'temp') {
                $this->_startnodes[] = $datanode['name'];
            } else if ($datanode['type'] == 'event' || $datanode['type'] == 'big_event') {
                $this->_endnodes[] = $datanode['name'];
            }
        }
    }

    private function _XmlToDepGen($dids) {
        $type = null;
        foreach ($dids as $did) {
            if (in_array($this->_idnode[$did], $this->_startnodes)) {
                $flowid = $this->_startnode2flowid[$this->_idnode[$did]];
                $dataFlow = $this->_flowid2flow[$flowid];
                if (array_key_exists('node_distinct', $dataFlow['function']) && $this->_nodeid[$dataFlow['function']['node_distinct']] === $did) {
                    $type = DBDep::TYPE_DISTINCT;
                } else {
                    $type = DBDep::TYPE_DEFAULT;
                }
                $dep = EtlVersionTool::createDep($this->_startnode2flowid[$this->_idnode[$did]], $did, $type);
            }

            if (in_array($this->_idnode[$did], $this->_endnodes)) {
                $type = DBDep::TYPE_DEFAULT;
                $gen = EtlVersionTool::createGen($this->_endnode2flowid[$this->_idnode[$did]], $did, $type);
            }
        }
    }

    private function _XmlToEtlVersion($eid) {
        //$name = $this->_args['name'];
        $name = $this->_dag['dag']['name'];
        if (array_key_exists('version_name', $this->_dag['dag'])) {
            $name = $this->_dag['dag']['version_name'];
        }
        $submitTime = time();
        $startTime = 0;
        $endTime = 0;
        $creator = $this->_args['creator'];
        $status = (int)$this->_args['status'];
        $comment = $this->_args['comment'];
        $deadline = GetDeadLine($this->_dag['dag']['dead_line']);
        $args['papi_version'] = (int)$this->_dag['dag']['papi_version'];
        $args['etl-framework_version'] = (int)$this->_dag['dag']['etl-framework_version'];

        $etlversion = EtlVersionTool::createEtlVersion($eid, $name, $submitTime, $startTime,
            $endTime, $creator, $status, $comment, $deadline, $args);
        if ($etlversion === null) {
            throw new ImporterException(__FILE__, __FUNCTION__, __LINE__, "Insert ETLVersion Failed");
        }
        $this->_evid = $etlversion->evid();
        return $etlversion->evid();
    }

    private function _SetStartEndNodes($dataFlow) {
        $startnodes = $dataFlow['startnodes'];
        $endnodes = $dataFlow['endnodes'];

        foreach ($startnodes as $startnode) {
            if (in_array($startnode['name'], $this->_startnodes)) {
                continue;
            } else {
                $this->_startnodes[] = $startnode['name'];
            }
        }
        foreach ($endnodes as $endnode) {
            if (in_array($endnode['name'], $this->_endnodes)) {
                continue;
            } else {
                $this->_endnodes[] = $endnode['name'];
            }
        }
    }

    private function _XmlToFlow($evid, $dataFlow, $soid, $fileid, $sfileid) {
        $function = $dataFlow['function'];
        $type = DBFlow::typeStrToInt($function['type']);
        $name = "";
        if (array_key_exists('name', $dataFlow['attribute'])) {
            $name = $dataFlow['attribute']['name'];
        }
        $x = 0;
        $y = 0;
        if (array_key_exists('x', $dataFlow['attribute'])) {
            $x = (int)$dataFlow['attribute']['x'];
        }
        if (array_key_exists('y', $dataFlow['attribute'])) {
            $y = (int)$dataFlow['attribute']['y'];
        }
        $args = array();
        self::_SetStartEndNodes($dataFlow);
        foreach (DBFlow::getArgOptList() as $flowarg) {
            if (array_key_exists($flowarg, $function)) {
                $args[$flowarg] = $function[$flowarg];
            }
        }

        try {
            $flow = EtlVersionTool::createFlow($evid, $type, $name, $x, $y, $soid, $fileid, $sfileid, $args);
            $flowid = $flow->fid();
            $this->_flowid2flow[$flowid] = $dataFlow;
            return $flowid;
        } catch (Exception $ex) {
            $exi = $ex->getMessage();
            $info = "$exi: evid = $evid, name = $name";
            throw new ImporterException(__FILE__, __FUNCTION__, __LINE__, $info);
        }
    }

    private $_dag;
    private $_startnodes;
    private $_endnodes;
    private $_args;
    private $_filename;
    private $_soid;
    private $_evid;
    private $_fileid;
    private $_sfileid;
    private $_flowid;
    private $_sodir;
    private $_shdir;
    private $_filedir;
    private $_idnode;
    private $_nodeid;
    private $_uploadso;
    private $_dispflow;
    private $_startnode2flowid;
    private $_endnode2flowid;
    private $_flowid2flow;
};

class MappingXmlImporter extends Importer {
    function __construct($fileName, $directory, $importer) {
        parent::__construct($fileName, $directory);
        $this->_importer = $importer;
    }

    function ImportXml() {
        $xml = parent::GetXml();
        $onefieldmap = array();
        foreach ($xml->fieldmap as $fieldmap) {
            $fieldname = "";
            $fieldMapAttr = self::_ImportMapAttr($fieldmap, $fieldname);
            $onefieldmap['name'] = $fieldMapAttr;
            $onefieldmap["type"] = self::_ImportType($fieldmap);
            $this->_map[] = $onefieldmap;
        }
        $reducefield = array();
        foreach ($xml->fieldreduce as $fieldmap) {
            $arr = array();
            foreach ($fieldmap->attributes() as $key => $value) {
                $arr[$key] = (string)$value;
            }
            $reducefield['name'] = $arr;
            $arrs = array();
            foreach ($fieldmap->fieldMap as $fieldmap) {
                $arr = array();
                foreach ($fieldmap->attributes() as $key => $value) {
                    $arr[$key] = (string)$value;
                }
                $arrs[] = $arr;
            }
            $reducefield['reduce'] = $arrs;
            $this->_map[] = $reducefield;
        }
    }

    private function _ImportMapAttr($fieldmap, &$name) {
        $fieldMapArr = array();
        foreach ($fieldmap->attributes() as $key => $value) {
            if ($key === "name") {
                $name = (string)$value;
            }
            $fieldMapArr[$key] = (string)$value;
        }
        return $fieldMapArr;
    }

    private function _ImportType($fieldmap) {
        $arrs = array();
        foreach ($fieldmap as $type) {
            $arr = array();
            foreach ($type->attributes() as $key => $value) {
                $arr[$key] = (string)$value;
            }
            $fieldMap = self::_ImportFieldMap($type);
            $arr["fieldMap"] = $fieldMap;
            $arrs[] = $arr;
        }
        return $arrs;
    }

    private function _ImportFieldMap($types) {
        $arrs = array();
        foreach ($types as $type) {
            $arr = array();
            foreach ($type->attributes() as $key => $value) {
                $arr[$key] = (string)$value;
            }
            $arrs[] = $arr;
        }
        return $arrs;
    }

    public function XmlToSql() {
        self::_MapTypeToSql();
    }

    private function _SetDataNodeConf($srcSchema, $conf) {
        $conf = GetConf($conf);
        $datanodeid = $this->_importer->GetNodeId();
        $did = $datanodeid[$srcSchema];
        EtlVersionTool::updateDatanodeConf($did, $conf);
    }

    private function _MapTypeToSql() {
        $startnode2flowid = $this->_importer->GetStartNode2FlowId();
        foreach($this->_map as $map) {
            if (!array_key_exists('reduce', $map)) {
                $types = $map['type'];
                $srcSchema = $map['name']['srcSchema'];
                $conf = $map['name']['conf'];
                self::_SetDataNodeConf($srcSchema, $conf);
                $flowid = $this->_importer->GetFlowId();
                $index = 0;
                foreach ($types as $type) {
                    $srctype = $type['name'];
                    $tgtschema = $type['tgtSchema'];
                    $outkey = $type['key'];
                    $fieldMap = $type['fieldMap'];
                    $maptype = FlowTool::addMappingType($startnode2flowid[$srcSchema], $srctype, $outkey, $tgtschema);
                    $mtid = $maptype->mtid();
                    self::_FieldMapToSql($mtid, $fieldMap);
                }
            } else {
                $name = $map['name'];
                $srcSchemas = $map['name']['srcSchema'];
                $tgtSchema = $map['name']['tgtSchema'];
                $so = $map['name']['so'];
                $flowid = $this->_importer->GetFlowId();
                $outkey = "";
                $srctype = "";
                $srcSchema = explode(',', $srcSchemas);
                if (count($srcSchema) === 1 && $srcSchema[0] === "") {
                    $srcSchema = array();
                }
                $maptype = FlowTool::addJoinMappingType($startnode2flowid[$srcSchema[0]], $tgtSchema);
                $mtid = $maptype->mtid();
                $reduces = $map['reduce'];
                self::_FieldMapToSql($mtid, $reduces);
            }
        }
    }

    private function _FieldMapToSql($mtid, $fieldMap) {
        foreach ($fieldMap as $fm) {
            $from = $fm['from'];
            $srcFields = explode(',', $from);
            if (count($srcFields) === 1 && $srcFields[0] === "") {
                $srcFields = array();
            }
            $to = $fm['to'];
            $compFunc = $fm['computFun'];
            $tgtFields = explode(',', $to);
            $checkFunc = "";
            if (array_key_exists('checkFun', $fm)) {
                $checkFunc = $fm['checkFun'];
            }
            $fm = FlowTool::addFieldMapping($mtid, $compFunc, $checkFunc, $srcFields, $tgtFields);
        }
    }   

    private $_map;
    private $_importer;
};

class DispatchXmlImporter extends Importer {
    function __construct($fileName, $directory, $importer) {
        parent::__construct($fileName, $directory);
        $this->_importer = $importer;
    }

    function ImportXml() {
        $xml = parent::GetXml();
        $onefieldmap = array();
        foreach ($xml->disp as $disp) {
            $dispname = "";
            $dispAttr = self::_ImportDispAttr($disp, $dispname);
            $onefieldmap[$dispname] = $dispAttr;
            $conditions = self::_ImportDispCondition($disp);
            $onefieldmap['condition'] = $conditions;
            $this->_disp[] = $onefieldmap;
        }
        return $this->_disp;
    }

    private function _ImportDispAttr($disp, &$name) {
        $dispArr = array();
        foreach ($disp->attributes() as $key => $value) {
            if ($key === "disp_name") {
                $name = 'disp';
            }
            $dispArr[$key] = (string)$value;
        }
        return $dispArr;
    }

    private function _ImportDispCondition($disp) {
        $conditions = array();
        foreach ($disp->condition as $condition) {
            $dispArr = array();
            foreach ($condition->attributes() as $key => $value) {
                $dispArr[$key] = (string)$value;
            }
            $conditions[] = $dispArr;
        }
        return $conditions;
    }

    function XmlToSql() {
        $this->_dispflow = $this->_importer->GetDispFlow();
        foreach ($this->_disp as $disp) {
            self::_DispToSql($disp);
        }
    }

    private function _DispToSql($disp) {
        $dispname = self::GetFile();
        $dispattr = $disp['disp'];       
        $nodeid = $this->_importer->GetNodeId();
        $name = $dispattr['disp_name'];
        $srcSchema = $dispattr['src_schema'];
        $tgtSchema = $dispattr['tgt_schema'];
        $did = $nodeid[$tgtSchema];

        $dispatch = FlowTool::addDispatching($this->_dispflow[$dispname], $did, $srcSchema, $name);
        $dispid = $dispatch->dspid();
        $this->_nameid[$name] = $dispid;
        self::_DispCondToSql($disp, $dispid);
    }

    private function _DispCondToSql($disp, $dispid) {
        $dispconds = $disp['condition'];
        foreach ($dispconds as $dispcond) {
            $condField = $dispcond['cond_field'];
            $condFields = explode(',', $condField);
            $condFunc = $dispcond['cond_func'];
            $depid = null;
            $deptype = 0;
            $dispcond = FlowTool::addDispatchCond($dispid, $condFields, $condFunc, $depid, $deptype);
        }
    }

    private $_disp;
    private $_importer;
    private $_nameid;
    private $_flowid;
};

