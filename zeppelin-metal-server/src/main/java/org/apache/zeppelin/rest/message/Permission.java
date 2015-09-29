package org.apache.zeppelin.rest.message;

/**
 * 
 * @author vgmartinez
 *
 */
public class Permission {
  String noteId;
  String permission;

  public Permission() {}

  public String getNoteId() {
    return noteId;
  }

  public String getPermission() {
    return permission;
  }
}
