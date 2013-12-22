<?php
/**
 * @file Job.php
 * @author zhangliang(com@baidu.com)
 * date 2013/12/07 19:16:45
 * @brief
 *
 **/
 
 require_once dirname(__FILE__)."/DagStruct.php";
 require_once dirname(__FILE__) . "/ConstDef.php";
 
 class Job {
 	public function __construct($input) {
 		$this->dagattr = $input->dagattr;
 		$this->startnodes = $input->startnodes;
 		$this->endnodes = $input->endnodes;
 		$this->flows = $input->flows;
 		$this->datanodes = $input->datanodes;
 	}
 	
 	public function getQueue() {
 		return $this->queue;
 	}
 	
 	public function excute() {
 		self::labelDataFlow();
        self::genDependence();
 		self::genQueue();
 	}
 	
    private function labelDataFlow() {
        foreach ($this->flows as $flow) {
            $isNodesReady = true;
            foreach ($flow->startnodes as $startnode) {
                if (in_array($startnode->name, $this->startnodes)) {
                    $startnode->isready = true;
                    $startnode->isreadyexec = true;
                } else {
                    $isNodesReady = false;
                    $startnode->isready = false;
                }
            }
            foreach ($flow->endnodes as $endnode) {
                $endnode->isready = false;
            }
            if ($isNodesReady === true) {
                $flow->isready = true;
                $flow->isreadyexec = true;
            } else {
                $flow->isready = false;
            }
        }
    } 
 	
 	private function isDepend($leftnodes, $rightnodes) {
 		$isdep = false;
 		foreach ($leftnodes as $leftnode) {
 			foreach ($rightnodes as $rightnode) {
 				if ($leftnode->name === $rightnode->name) {
 					$isdep = true;
 				}
 			}
 		}
 		return $isdep;
 	}
 	
 	private function genDependence() {
 		$index1 = 0;
 		$index2 = 0;
 		for ($index1 = 0; $index1 < count($this->flows); $index1++) {
 			for ($index2 = 0; $index2 < count($this->flows); $index2++) {
 				if ($index1 === $index2) {
 					continue;
 				}
 				if (self::isDepend($this->flows[$index1]->startnodes, $this->flows[$index2]->endnodes)) {
 					$this->flows[$index1]->depindexs[] = $index2;
 				}
 			}
 		}
 	}

 	private function genQueue() {
 		$index = 0;
 		while (true) {
 			foreach ($this->flows as $flow) {
 				$isNodesReady = true;
 				foreach ($flow->startnodes as $startnode) {
 					if ($startnode->isready === false) {
 						$isNodesReady = false;
 					}
 				}
 				if ($isNodesReady === true) {
 					$flow->isready = true;
 				} else {
 					$flow->isready = false;
 				}
 				if ($flow->isready === true) {
 					foreach ($flow->endnodes as $endnode) {
 						$endnode->isready = true;
 					}
 					if ($flow->ispushed === false && self::isNeedPushIn($flow)) {
 						$this->queue[$index][] = $flow;
 						$flow->ispushed = true;
 						foreach ($flow->depindexs as $depindex) {
 							$this->queue[$index][] = $this->flows[$depindex];
 							$this->flows[$depindex]->ispushed = true;
 						}
 						$index++; 			
 					}
 				}
 			}
 			if (self::shouldstop($this->flows)) {
 				break;
 			}
 		}
 	}
 	
 	private function shouldstop($flows) {
 		$stop = true;
 		foreach ($flows as $flow) {
 			if ($flow->ispushed === false) {
 				$stop = false;
 				break;
 			}
 		}
 		return $stop;
 	}
 	
 	private function isNeedPushIn($flow) {
 		$ret = false;
 		do {
 			if ($flow->type === ConstDef::UNION || $flow->type === ConstDef::JOIN || $flow->type === ConstDef::ANTISPAM) {
 				$ret = true;
 				break;
 			}
 			if ($flow->type === ConstDef::SINGLEMAP) {
 				$istemp = false;
 				foreach ($flow->endnodes as $endnode) {
 					if ($endnode->type === ConstDef::TEMP) {
 						$istemp = true;
 						break;
 					}
 				}
 				if ($istemp === false) {
 					$ret = true;
 				} else {
 					$ret = false;
 				}
 			} 
 		} while (0);
 		
 		return $ret;
 	}
 		
 	private $flows;
 	private $startnodes;
 	private $endnodes;
 	private $datanodes;
 	private $dagattr;
 	private $queue;
 };
