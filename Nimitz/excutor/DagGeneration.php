<?php
/**
 * @file DagGeneration.php
 * @author zhangliang(com@baidu.com)
 * date 2013/12/10 12:56:32
 * @brief
 *
 **/
 
 require_once dirname(__FILE__)."/DagStruct.php";
 require_once dirname(dirname(__FILE__)).'/components/DagInfoComponents.php';
 
 class DagGeneration {
 	public function __construct($eid, $evid) {
 		$this->excutorComponents = new DagInfoComponent($eid, $evid);
 		$this->dag = new Dag();
 	}
 	
 	public function getDag() {
 		return $this->dag;
 	}
 	
	public function getStartNodes() {
		$startnodes = $this->excutorComponents->getStartNodes();
		$this->dag->startnodes = $startnodes; 
	}
	
	public function getEndNodes() {
		$endnodes = $this->excutorComponents->getEndNodes();
		$this->dag->endnodes = $endnodes;
	}
	
	public function getDataNodes() {
		$datanodes = $this->excutorComponents->getDataNodes();
		$nodes = array();
		foreach ($datanodes as $datanode) {
            $node = null;
            if ($datanode[ConstDef::TYPE] === ConstDef::TEXTLOG || $datanode[ConstDef::TYPE] === ConstDef::LOG || $datanode[ConstDef::TYPE] === ConstDef::COMBINETEXTLOG || $datanode[ConstDef::TYPE] === ConstDef::COMBINELOG) {
                $node = new Log();
                self::setDataNode($node, $datanode);
                $node->deptype = $datanode[ConstDef::DEPTYPE];
                $node->gentype = $datanode[ConstDef::GENTYPE];
                $node->init();
            } else if ($datanode[ConstDef::TYPE] === ConstDef::EVENT || $datanode[ConstDef::TYPE] === ConstDef::MV || $datanode[ConstDef::TYPE] === ConstDef::BIGEVENT) {
                $node = new Table();
                if (array_key_exists($datanode[ConstDef::NAME], $this->dag->startnodes)) {
                    $node->isstart = true;
                } 
                self::setDataNode($node, $datanode);
                $node->deptype = $datanode[ConstDef::DEPTYPE];
                $node->gentype = $datanode[ConstDef::GENTYPE];
                $node->init();
            } else {
                $node = new TempNode();
                $node->name = $datanode[ConstDef::NAME];
                $node->type = $datanode[ConstDef::TYPE];
                $node->nodedistinct = $datanode[ConstDef::NODEDISTINCT];
                $node->did = $datanode[ConstDef::DID];
            }
            $nodes[] = $node;	
            $this->did2datanode[$datanode[ConstDef::DID]] = $node;
		}
		$this->dag->datanodes = $nodes;
	}

    private function setDataNode(&$destNode, $srcNode) {
        $destNode->name = $srcNode[ConstDef::NAME];
        $destNode->did = $srcNode[ConstDef::DID];
        $destNode->product = $srcNode[ConstDef::PRODUCT];
        $destNode->type = $srcNode[ConstDef::TYPE];
        if (array_key_exists(ConstDef::DEPTIME, $srcNode[ConstDef::DEPTIME])) {
            $destNode->deptime = $srcNode[ConstDef::DEPTIME][ConstDef::DEPTIME];
        }
        $destNode->fsname = $srcNode[ConstDef::DATANODEFSNAME];
        if ($srcNode[ConstDef::PARTITION] != null && count($srcNode[ConstDef::PARTITION]) !== 0) {
            foreach ($srcNode[ConstDef::PARTITION] as $parts) {
                $pt = "";
                $first = true;
                foreach ($parts as $key => $value) {
                    if ($first === true) {
                        $pt = $pt . $key . "=" . $value;
                        $first = false;
                    } else {
                        $pt = $pt . "." . $key . "=" . $value;
                    }
                }
                $destNode->partition[] = $pt;
            }
        } else {
            throw new Exception("Dag Partition Error");
        }
    }
	
	public function getDagAttr() {
		$dagattr = $this->excutorComponents->getDagAttr();
		$dagattrstruct = new DagAttr();
		$dagattrstruct->name = $dagattr[ConstDef::NAME];
		$dagattrstruct->freq = $dagattr[ConstDef::FREQ] / 60;
		$dagattrstruct->start_partition = date("Y-m-d H:i:s", $dagattr[ConstDef::STARTPARTITION]);
		$dagattrstruct->end_partition = date("Y-m-d H:i:s", $dagattr[ConstDef::ENDPARTITION]);
		$dagattrstruct->warning_list = $dagattr[ConstDef::CONCERNERS];
		$dagattrstruct->version_name = $dagattr[ConstDef::VERSIONNAME];
        $dagattrstruct->dead_line = $dagattr[ConstDef::DEADLINE];
        if (array_key_exists(ConstDef::ETLFRAMEWORKVERSION, $dagattr[ConstDef::ARGS])) {
            $dagattrstruct->etl_framework_version = $dagattr[ConstDef::ARGS][ConstDef::ETLFRAMEWORKVERSION];
        }
        if (array_key_exists(ConstDef::PAPIVERSION, $dagattr[ConstDef::ARGS])) {
            $dagattrstruct->papi_version = $dagattr[ConstDef::ARGS][ConstDef::PAPIVERSION];
        }
		self::getClusterInfo($dagattr);
        $this->dag->dagattr = $dagattrstruct;
	}
	
	public function getClusterInfo($dagattr) {
		$cluster = new Cluster();
		$cluster->namenode = $dagattr[ConstDef::NAMENODE];
		$cluster->jobtracker = $dagattr[ConstDef::JOBTRACKER];
		$cluster->lognamenode = $dagattr[ConstDef::LOGNAMENODE];
		$cluster->displayer = $dagattr[ConstDef::DISPLAYER];
		$cluster->queue = $dagattr[ConstDef::QUEUE];
		$ugi = $dagattr[ConstDef::UGI];
		$arr = explode(',', $ugi);
		if (count($arr) !== 2) {
			throw new Exception("UGI Format Error");
		}
		$cluster->user = $arr[0];
		$cluster->passwd = $arr[1];
		$this->dag->cluster = $cluster;
	}
	
	public function getFlows() {
		$flows = array();
		$flowarrs = $this->excutorComponents->getFlows();
		$index = 0;
		foreach ($flowarrs as $flowarr) {
			$flowattr = new FlowAttr();
			$flow = new FLow();
            $startnode = null;
            $endnode = null;
			$flow->index = $index;
			$flow->fid = $flowarr[ConstDef::FID];
			$flow->type = $flowarr[ConstDef::TYPE];
			$startnodes = $flowarr[ConstDef::STARTDID];
			foreach ($startnodes as $did) {
				$startnode = $this->did2datanode[$did];
				$startnode->isready = false;
				$startnode->isreadyexec = false;
                $flow->startnodes[] = $startnode;
			}
			$endnodes = $flowarr[ConstDef::ENDDID];
			foreach ($endnodes as $did) {
				$endnode = $this->did2datanode[$did];
				$endnode->isready = false;
				$endnode->isreadyexec = false;
                $flow->endnodes[] = $endnode;
			}
			if (array_key_exists(ConstDef::SO, $flowarr)) {
				$flowattr->so = $flowarr[ConstDef::SO];
			} else {
				$flowattr->so = null;
			}
			if (array_key_exists(ConstDef::XML, $flowarr)) {
				$flowattr->xml = $flowarr[ConstDef::XML];
			} else {
				$flowattr->xml = null;
			}
			if (array_key_exists(ConstDef::DISP, $flowarr)) {
				$flowattr->disp = $flowarr[ConstDef::DISP];
			} else {
				$flowattr->disp = null;
			}
			if (array_key_exists(ConstDef::SHELL, $flowarr)) {
				$flowattr->shfile = $flowarr[ConstDef::SHELL];
			} else {
				$flowattr->shfile = null;
			}
			if (array_key_exists(ConstDef::OTHERFILE, $flowarr)) {
				$flowattr->otherfile = $flowarr[ConstDef::OTHERFILE];
			} else {
				$flowattr->otherfile = null;
			}
			$flowattr->args = $flowarr[ConstDef::ARGS];
			$flow->flowattr = $flowattr;
			$flow->isready = false;
			$flow->isreadyexec = false;
			$flows[] = $flow;
		}
		$this->dag->flows = $flows;
	}
 	
 	public function genDag() {
 		self::getStartNodes();
 		self::getEndNodes();
 		self::getDataNodes();
 		self::getFlows();
        self::getDagAttr();
        return $this->dag;
 	}
 	
 	private $excutorComponents;
 	private $startnodes;
 	private $endnodes;
 	private $dag;	
    private $did2datanode;
 };
 
