import collections
import datetime
import getpass
import numpy as np
import string

import gdata.spreadsheet.service

def _get_gdata_id(str):
  return str.split('/')[-1]

def _get_gdata_feedlist(feed):
  attrs = ['id', 'title', 'last_updated']
  SpreadsheetInfo = collections.namedtuple('SpreadsheetsInfo', attrs)
  
  sheets = []
  for sheet in feed.entry:
    id = _get_gdata_id(sheet.id.text)
    title = sheet.title.text
    datestr, format = sheet.updated.text, '%Y-%m-%dT%H:%M:%S.%fZ'
    date_modified = datetime.datetime.strptime(datestr, format)
    
    info = SpreadsheetInfo(id, title, date_modified)
    sheets.append(info)
    
  return sheets

class SpreadsheetsConnection(object):
  def __init__(self):
    self.svc = gdata.spreadsheet.service.SpreadsheetsService()
    self.spreadsheet_id = ''
    self.worksheet_id = ''
        
  def login(self, user, passwd):
    self.svc = gdata.spreadsheet.service.SpreadsheetsService()
    self.svc.email = user
    self.svc.password = passwd
    self.svc.ProgrammaticLogin()
    
  def get_spreadsheet_list(self):
    return _get_gdata_feedlist(self.svc.GetSpreadsheetsFeed())
    
  def get_spreadsheet(self, id):
    return GSpreadsheet(self.svc, id)

class GSpreadsheet(object):
  def __init__(self, svc, id):
    self.svc = svc
    self.id = id
  
  def get_worksheet_list(self):
    return _get_gdata_feedlist(self.svc.GetWorksheetsFeed(self.id))
  
  def get_worksheet(self, id):
    return GWorksheet(self.svc, self.id, id)

class GWorksheet(object):
  def __init__(self, svc, ssid, wsid):
    self.svc = svc
    self.ssid = ssid
    self.wsid = wsid
    
    self.cells_feed = None
    self.cell_data = None
    self.update_mask = None
    
    self._has_header_row, self._has_header_col = False, False
    self.pull()
  
  def _get_has_header_row(self):
    return self._has_header_row
  
  def _get_has_header_col(self):
    return self._has_header_col
  
  def _set_has_header_row(self, has_header_row):
    if has_header_row and not self._has_header_row: # Adding header row
      self._colnames = list(self.cell_data[0,:])
      if self.has_header_col:
        self._colnames.insert(0, self._rownames[0])
      self.cell_data = np.delete(self.cell_data, 0, 0)
      self.update_mask = np.delete(self.update_mask, 0, 0)
    
    if not has_header_row and self._has_header_row: # Removing header row
      colnames = self._colnames[self.has_header_col:]
      self.cell_data = np.insert(self.cell_data, 0, colnames, 0)
      self.update_mask = np.insert(self.update_mask, 0, False, 0)
      self._colnames = None
    
    if self._has_header_row != has_header_row:
      self._has_header_row = has_header_row
      self._update_colname_map()
  
  def _set_has_header_col(self, has_header_col):
    if has_header_col and not self._has_header_col: # Adding header col
      self._rownames = list(self.cell_data[:,0])
      if self.has_header_row:
        self._rownames.insert(0, self._colnames[0])
      self.cell_data = np.delete(self.cell_data, 0, 1)
      self.update_mask = np.delete(self.update_mask, 0, 1)
    
    if not has_header_col and self._has_header_col: # Removing header col
      rownames = self._rownames[self.has_header_row:]
      self.cell_data = np.insert(self.cell_data, 0, rownames, 1)
      self.update_mask = np.insert(self.update_mask, 0, False, 1)
      self._rownames = None
    
    if self._has_header_col != has_header_col:
      self._has_header_col = has_header_col
      self._update_rowname_map()
  
  has_header_row = property(_get_has_header_row, _set_has_header_row)
  has_header_col = property(_get_has_header_col, _set_has_header_col)
  
  def set_has_headers(self, has_header_row, has_header_col):
    self.has_header_row = has_header_row
    self.has_header_col = has_header_col
  
  def _update_colname_map(self):
    if self.has_header_row:
      self._colname_inds = {name.lower() if name is not None else None: i
                              for (i, name) in enumerate(self._colnames)}
    else: self._colname_inds = None
  
  def _update_rowname_map(self):
    if self.has_header_col:
      self._rowname_inds = {name.lower() if name is not None else None: i
                              for (i, name) in enumerate(self._rownames)}
    else: self._rowname_inds = None
  
  def pull(self):
    query = gdata.spreadsheet.service.CellQuery()
    query.return_empty = "true"
    self.cells_feed = self.svc.GetCellsFeed(self.ssid, self.wsid, query=query)
    nrows = string.atoi(self.cells_feed.row_count.text)
    ncols = string.atoi(self.cells_feed.col_count.text)
    self.cell_data = np.empty((nrows, ncols), dtype=object)
    self.update_mask = np.zeros((nrows, ncols), dtype=bool)
      
    for e in self.cells_feed.entry:
      row = string.atoi(e.cell.row) - 1
      col = string.atoi(e.cell.col) - 1
      self.cell_data[row, col] = e.cell.text
    
    old_has_header_row = self._has_header_row
    old_has_header_col = self._has_header_col
    self._has_header_row, self._has_header_col = False, False
    self._colnames, self._rownames = None, None
    self._colname_inds, self._rowname_inds = None, None
    self.set_has_headers(old_has_header_row, old_has_header_col)
  
  def push(self):
    batch_request = gdata.spreadsheet.SpreadsheetsCellsFeed()
    
    for row, col in map(tuple, np.argwhere(self.update_mask)):
      val = self.cell_data[row, col]
      real_row, real_col = row + self.has_header_row, col + self.has_header_col
      ind = real_row*(self.num_cols() + self.has_header_col) + real_col
      self.cells_feed.entry[ind].cell.inputValue = val
      batch_request.AddUpdate(self.cells_feed.entry[ind])
    
    self.svc.ExecuteBatch(batch_request, self.cells_feed.GetBatchLink().href)
    self.update_mask[:,:] = False
  
  def num_rows(self):
    return np.size(self.cell_data, 0)
  
  def num_cols(self):
    return np.size(self.cell_data, 1)
  
  def row_exists(self, name):
    return self.has_header_col and name in self._rowname_inds
  
  def col_exists(self, name):
    return self.has_header_row and name in self._colname_inds
  
  def _do_lookup(self, ind, name_map, has_other_header, kind):
    if isinstance(ind, str):
      if name_map is None:
        raise TypeError('Sheet not set to have a header {0}'.format(kind))
      return name_map[string.lower(ind)] - has_other_header
    return ind
  
  def _get_row_ind(self, row):
    has_header = self.has_header_row
    f = lambda r: self._do_lookup(r, self._rowname_inds, has_header, 'column')
    return map(f, row) if isinstance(row, list) else f(row)
  
  def _get_col_ind(self, col):
    has_header = self.has_header_col
    f = lambda c: self._do_lookup(c, self._colname_inds, has_header, 'row')
    return map(f, col) if isinstance(col, list) else f(col)

  def __getitem__(self, (row, col)):
    row, col = self._get_row_ind(row), self._get_col_ind(col)
    return self.cell_data[row, col]

  def __setitem__(self, (row, col), val):
    row, col = self._get_row_ind(row), self._get_col_ind(col)
    self.cell_data[row, col] = str(val)
    self.update_mask[row, col] = True
    