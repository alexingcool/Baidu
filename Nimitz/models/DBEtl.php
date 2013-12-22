<?php
/**
 * @file DBEtl.php
 * @author zhangliang(com@baidu.com)
 * @date 2013/11/05 15:17:01
 * @brief 
 *  
 **/

require_once dirname(dirname(__FILE__)).'/components/DBConnect.php';
require_once dirname(dirname(__FILE__)).'/components/NimitzLog.php';

/**
 * @brief the etl
 */
class DBEtl {
    public function eid() { ///< getter
        return $this->_eid;
    }
    public function name() { ///< getter
        return $this->_name;
    }
    public function nid() { ///< getter
        return $this->_nid;
    }
    public function interval() { ///< getter
        return $this->_interval;
	}
	public function start_time() {
		return $this->_start_time;
	}
	public function end_time() {
		return $this->_end_time;
	}
	public function auth_user() {
		return $this->_auth_user;
	}
	public function op_user() {
		return $this->_op_user;
	}
	public function comment() {
		return $this->_comment;
	}
	public function status() {
		return $this->_status;
	}
	public function jtid() {
		return $this->_jtid;
	}
	public function fsid() {
		return $this->_fsid;
	}
	public function deptype() {
		return $this->_deptype;
	}
	public function gentype() {
		return $this->_gentype;
	}
	public function jobid() {
		return $this->_jobid;
	}

    public function setName($name) { ///< setter
		if (is_string($name)) {
			if ($this->_name !== $name) {
           		$this->_name = $name;
            	$this->_changed = true;
			}
			return true;
		}
		return false;
    }

    public function setNid($nid) { ///< setter
		if (is_integer($nid)) {
		   if ($this->_nid !== $nid) {
			   $this->_nid = $nid;
			   $this->_changed = true;
		   }
		   return true;
		}
		return false;
    }

    public function setInterval($interval) { ///< setter
		if (is_integer($interval)) {
			if($this->_interval !== $interval) {
            	$this->_interval = $interval;
           		$this->_changed = true;
			}
			return true;
		}
		return false;
	}

	public function setStartTime($start_time) { ///< setter
		if (is_integer($start_time)) {
		   if ($this->_start_time !== $start_time) {
			   $this->_start_time = $start_time;
			   $this->_changed = true;
		   }
		   return true;
		} 
		return false;
	}

	public function setEndTime($end_time) { ///< setter
		if (is_integer($end_time)) {
		   if($this->_end_time !== $end_time) {
			   $this->_end_time = $end_time;
			   $this->_changed = true;
		   }
		   return true;
		} 
		return false;
	}

	public function setAuthUser($auth_user) { ///< setter
		if (is_string($auth_user)) {
		   if ($this->_auth_user !== $auth_user) {
			   $this->_auth_user = $auth_user;
			   $this->_changed = true;
		   }
		   return true;
		} 
		return false;
	}

	public function setOpUser($op_user) { ///< setter
		if (is_string($op_user)) {
			if ($this->_op_user !== $op_user) {
            	$this->_op_user = $op_user;
           		$this->_changed = true;
			}
			return true;
		} 
		return false;
	}

	public function setComment($comment) { ///< setter
		if (is_string($comment)) {
		   if ($this->_comment !== $comment) {
			   $this->_comment = $comment;
			   $this->_changed = true;
		   }
		   return true;
		}
		return false;
	}

	public function setStatus($status) { ///< setter
		if (is_integer($status)) {
		   if ($this->_status !== $status) {
			   $this->_status = $status;
			   $this->_changed = true;
		   }
		   return true;
		}
		return false;
	}

	public function setJtid($jtid) { ///< setter
		if (is_integer($jtid)) {
		   if ($this->_jtid !== $jtid) {
			   $this->_jtid = $jtid;
			   $this->_changed = true;
		   }
		   return true;
		}
		return false;
	}

	public function setFsid($fsid) { ///< setter
		if (is_integer($fsid)) {
		   if ($this->_fsid !== $fsid) {
			   $this->_fsid = $fsid;
			   $this->_changed = true;
		   }
		   return true;
		}
		return false;
	}

	public function setDepType($deptype) { ///< setter
		if (is_integer($deptype)) {
		   if ($this->_deptype !== $deptype) {
			   $this->_deptype = $deptype;
     		   $this->_changed = true;
		   }
		   return true;
		}
		return false;
	}

	public function setGenType($gentype) { ///< setter
		if (is_integer($gentype)) {
		   if ($this->_gentype !== $gentype) {
			   $this->_gentype = $gentype;
			   $this->_changed = true;
		   }
		   return true;
		}
		return false;
	}

	public function setJobid($jobid) { ///< setter
		if (is_integer($jobid)) {
		   if ($this->_jobid !== $jobid) {
			   $this->_jobid = $jobid;
			   $this->_changed = true;
		   }
		   return true;
		}
		return false;
	}

	public static function insertEtl($name, $nid, $interval, $start_time, $end_time, $auth_user, $op_user,
	   	$comment, $status, $jtid, $fsid, $deptype, $gentype, $jobid) {
		if (is_string($name) && is_integer($nid) && is_integer($interval) && is_integer($start_time) && is_integer($end_time) && 
			is_string($auth_user) && is_string($op_user) && is_string($comment) && (is_integer($status) || is_null($status)) && is_integer($jtid) 
			&& is_integer($fsid) && is_integer($deptype) && is_integer($gentype) && (is_integer($jobid) || is_null($jobid))) {
            $pdo = DBConnect::getPDO();
			$sql = sprintf(self::INSERT_SQL_PATTERN, $pdo->quote($name), $nid, $interval, 
			$start_time, $end_time, $pdo->quote($auth_user), $pdo->quote($op_user), 
			$pdo->quote($comment), $status, $jtid, $fsid, 
            $deptype, $gentype, $jobid);
            $ret = $pdo->exec($sql);
            if (1 != $ret) {
				$msgs = $pdo->errorInfo();
				UB_LOG_WARnING('[%s:%d] Insert Failed. [name:%s] [sql:%s] [err:%s:%d:%s] ',
					__FILE__, __LINE__, $name, $sql, $msgs[0], $msgs[1], $msgs[2]);
				return null;
            } else {
				$lastid = $pdo->lastInsertId();
				return new DBEtl($lastid, $name, $nid, $interval, $start_time, $end_time, $auth_user, $op_user, $comment, 
					$status, $jtid, $fsid, $deptype, $gentype, $jobid);
            }
        } else {
            UB_LOG_WARNING('[%s:%d] Invalid arguments. ',
                __FILE__, __LINE__);
            return null;
        }
    }

    public static function getById($eid) {
        if (is_integer($eid)) {
            $pdo = DBConnect::getPDO();
            $sql = sprintf(self::SELECT_SQL_PATTERN, $eid);
            $stt = $pdo->query($sql);
			if (false === $stt) {
				$msgs = $pdo->errorInfo();
				UB_LOG_WARNING('[%s:%d] Get By Id Failed. [name:%s] [sql:%s] [err:%s:%d:%s] ',
					__FILE__, __LINE__, $name, $sql, $msgs[0], $msgs[1], $msgs[2]);
				return null;
            } else {
                $rst = $stt->fetchAll(PDO::FETCH_NUM);
                if (1 != count($rst)) {
                    return null;
				} else {
					return new DBEtl($rst[0][0], $rst[0][1], $rst[0][2], $rst[0][3], $rst[0][4], $rst[0][5], $rst[0][6], $rst[0][7],
					$rst[0][8], $rst[0][9], $rst[0][10], $rst[0][11], $rst[0][12], $rst[0][13], $rst[0][14]);
                }
            }
        } else {
            UB_LOG_WARNING('[%s:%d] Invalid arguments. ',
                __FILE__, __LINE__);
            return null;
        }
    }

    public static function getByName($name) {
        if (is_string($name)) {
            $pdo = DBConnect::getPDO();
            $sql = sprintf(self::SELECT_NAME_SQL_PATTERN, $pdo->quote($name));
            $stt = $pdo->query($sql);
            if (false === $stt) {
                $msgs = $pdo->errorInfo();
                UB_LOG_WARNING('[%s:%d] Select Etl By Name Failed. [name:%s] [sql:%s] [err:%s:%d:%s] ',
                    __FILE__, __LINE__, $name, $sql, $msgs[0], $msgs[1], $msgs[2]);
                return null;
            } else {
                $rst = $stt->fetchAll(PDO::FETCH_NUM);
                if (1 != count($rst)) {
                    return null;
                } else {
                    return new DBEtl($rst[0][0], $rst[0][1], $rst[0][2], $rst[0][3], $rst[0][4], $rst[0][5], $rst[0][6], $rst[0][7],
                        $rst[0][8], $rst[0][9], $rst[0][10], $rst[0][11], $rst[0][12], $rst[0][13], $rst[0][14]);
                }
            }
        } else {
            UB_LOG_WARNING('[%s:%d] Invalid arguments. ',
                __FILE__, __LINE__);
            return null;
        }
    }

    public static function getByNid($nid) {
        $pdo = DBConnect::getPDO();
        $sql = sprintf(self::SELECT_NID_SQL_PATTERN, $nid);
        $stt = $pdo->query($sql);
        if (false === $stt) {
			$msgs = $pdo->errorInfo();
			UB_LOG_WARNING('[%s:%d] Insert Failed. [name:%s] [sql:%s] [err:%s:%d:%s] ',
				__FILE__, __LINE__, $name, $sql, $msgs[0], $msgs[1], $msgs[2]);
			return null;
        } else {
            $rst = $stt->fetchAll(PDO::FETCH_NUM);
            $etlArray = array();
            foreach($rst as $row) {
				$etlArray[$row[0]] = new DBEtl($row[0], $row[1], $row[2], $row[3], $row[4], $row[5], $row[6], $row[7],
					$row[8], $row[9], $row[10], $row[11], $row[12], $row[13], $row[14]);
            }
            return $etlArray;
        }
    }

	public static function getByNidAndEid($nid, $eid) {
		$pdo = DBConnect::getPDO();
		$sql = sprintf(self::SELECT_NIDEID_SQL_PATTERN, $nid, $eid);
		$stt = $pdo->query($sql);
		if (false === $stt) {
			$msgs = $pdo->errorInfo();
			UB_LOG_WARNING('[%s:%d] Insert Failed. [name:%s] [sql:%s] [err:%s:%d:%s] ',
				__FILE__, __LINE__, $name, $sql, $msgs[0], $msgs[1], $msgs[2]);
			return null;
		} else {
			$rst = $stt->fetchAll(PDO::FETCH_NUM);
			$etlArray = array();
			foreach($rst as $row) {
				$etlArray[$row[0]] = new DBEtl($row[0], $row[1], $row[2], $row[3], $row[4], $row[5], $row[6], $row[7],
					$row[8], $row[9], $row[10], $row[11], $row[12], $row[13], $row[14]);
			}
			return $etlArray;
		}
	}

    public function delete() {
        $pdo = DBConnect::getPDO();
        $sql = sprintf(self::DELETE_SQL_PATTERN, $this->_eid);
        $ret = $pdo->exec($sql);
		if (1 != $ret) {
			$msgs = $pdo->errorInfo();
			UB_LOG_WARNING('[%s:%d] delete Failed. [eid:%d] [sql:%s] [err:%s:%d:%s] ',
				__FILE__, __LINE__, $this->_eid, $sql, $msgs[0], $msgs[1], $msgs[2]);
            return false;
        } else {
            return true;
        }
    }

	public static function deleteByNid($nid) {
		if (is_integer($nid)) {
			$pdo = DBConnect::getPDO();
			$sql = sprintf(self::DELETENID_SQL_PATTERN, $nid);
			printf("\nsql = %s\n", $sql);
			$ret = $pdo->exec($sql);
			if (1 != $ret) {
				$msgs = $pdo->errorInfo();
				UB_LOG_WARNING('[%s:%d] delete nid Failed. [name:%s] [sql:%s] [err:%s:%d:%s] ',
					__FILE__, __LINE__, $name, $sql, $msgs[0], $msgs[1], $msgs[2]);
				return false;
			} else {
				return true;
			}
		} else {
			return false;
		}
	}

    public function update() {
        if ($this->_changed) {
            $pdo = DBConnect::getPDO();
            $sql = sprintf(self::UPDATE_SQL_PATTERN, $pdo->quote($this->_name),
				$this->_nid, $this->_interval, $this->_start_time, 
				$this->_end_time, $pdo->quote($this->_auth_user), $pdo->quote($this->_op_user),$pdo->quote($this->_comment),
			   	$this->_status, $this->_jtid, $this->_fsid,
				$this->_deptype, $this->_gentype, $this->_jobid, $this->_eid);
            $ret = $pdo->exec($sql);
            if (1 != $ret) {
                $msgs = $pdo->errorInfo();
                UB_LOG_WARNING('[%s:%d] Update Failed. [jtid:%u] [sql:%s] [err:%s:%d:%s] ',
                    __FILE__, __LINE__, $this->_jtid, $sql, $msgs[0], $msgs[1], $msgs[2]);
                return false;
            } else {
                $this->_changed = false;
                return true;
            }
        } else {
            return true;
        }
    }

    /// constructor
	private function __construct($eid, $name, $nid, $interval, $start_time, $end_time, $auth_user, 
		$op_user, $comment, $status, $jtid, $fsid, $deptype, $gentype, $jobid) {
		$this->_eid = (integer)	$eid;			
		$this->_name = (string) $name;
		$this->_nid= (integer) $nid;
		$this->_interval = (integer) $interval;
		$this->_start_time = (integer) $start_time;
		$this->_end_time = (integer) $end_time;
		$this->_auth_user = (string) $auth_user;
		$this->_op_user = (string) $op_user;
		$this->_comment = (string) $comment;
		$this->_status = (integer) $status;
		$this->_jtid = (integer) $jtid;
		$this->_fsid = (integer) $fsid;
		$this->_deptype = (integer) $deptype;
		$this->_gentype = (integer) $gentype;
		$this->_jobid = (integer) $jobid;
        $this->_changed = false;
    }

    public static function getDepType($type) {
        return self::$DEPTYPEINT[$type];
    }

    public static function getGenType($type) {
        return self::$GENTYPEINT[$type];
    }

    /// SQL Pattern
    const INSERT_SQL_PATTERN = 'INSERT INTO etl(name, nid, `interval`, start_time, end_time, auth_user, 
		op_user, comment, status, jtid, fsid, deptype, gentype, jobid) 
		VALUES (%s, %d, %d, %d, %d, %s, %s, %s, %d, %d, %d, %d, %d, %d)';
    /// SQL Pattern
    const SELECT_SQL_PATTERN = 'SELECT eid, name, nid, `interval`, start_time, end_time, auth_user, op_user, comment, status, jtid, fsid, deptype, gentype, jobid from etl where eid=%d';
    const SELECT_NAME_SQL_PATTERN = 'SELECT eid, name, nid, `interval`, start_time, end_time, auth_user, op_user, comment, status, jtid, fsid, deptype, gentype, jobid from etl where name=%s';
    /// SQL Pattern
    const DELETE_SQL_PATTERN = 'DELETE FROM etl WHERE eid=%d';
    /// SQL
    const SELECT_NID_SQL_PATTERN = 'SELECT eid, name, nid, `interval`, start_time, end_time, auth_user, op_user, comment, status, jtid, fsid, deptype, gentype, jobid from etl where nid=%d';
    /// SQL Pattern
    const UPDATE_SQL_PATTERN = 'UPDATE etl SET name=%s, nid=%d, `interval`=%d, start_time=%d, end_time=%d, auth_user=%s, 
		op_user=%s, comment=%s, status=%d, jtid=%d, fsid=%d, deptype=%d, gentype=%d, jobid=%d WHERE eid=%d';
	///SQL Nid Pattern
	const DELETENID_SQL_PATTERN = 'DELETE FROM etl WHERE nid=%d';

	///SQL NID EID Pattern
	const SELECT_NIDEID_SQL_PATTERN = 'SELECT eid, name, nid, `interval`, start_time, end_time, auth_user, op_user, 
		comment, status, jtid, fsid, deptype, gentype, jobid from etl where nid=%d AND eid=%d';

    private static $GENTYPESTR = array (
        'global' => 0,
        'local' => 1,
    );
    private static $DEPTYPESTR = array (
        'global' => 0,
        'active' => 1, 
        'passive' => 2,
    );
    private static $GENTYPEINT = array (
        0 => 'global',
        1 => 'local',
    );
    private static $DEPTYPEINT = array (
        0 => 'global',
        1 => 'active',
        2 => 'passive',
    );

	private $_eid;
	private $_name;
	private $_nid;
	private $_interval;
	private $_start_time;
	private $_end_time;
	private $_auth_user;
	private $_op_user;
	private $_comment;
	private $_status;
	private $_jtid;
	private $_fsid;
	private $_deptype;
	private $_gentype;
	private $_jobid;
}



