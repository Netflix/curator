package com.netflix.curator.framework.recipes.leader;

/**
 * A LeaderLatchListener can be used to be notified asynchronously about when the state of the LeaderLatch has changed.
 *
 * Note that just because you are in the middle of one of these method calls, it does not necessarily mean that
 * hasLeadership() is the corresponding true/false value.  It is possible for the state to change behind the scenes
 * before these methods get called.  The contract is that if that happens, you should see another call to the other
 * method pretty quickly.
 */
public interface LeaderLatchListener
{
  /**
   * This is called when the LeaderLatch's state goes from hasLeadership = false to hasLeadership = true.
   *
   * Note that it is possible that by the time this method call happens, hasLeadership has fallen back to false.  If
   * this occurs, you can expect stopBeingMaster() to also be called.
   */
  public void becomeMaster();

  /**
   * This is called when the LeaderLatch's state goes from hasLeadership = true to hasLeadership = false.
   *
   * Note that it is possible that by the time this method call happens, hasLeadership has become true.  If
   * this occurs, you can expect becomeMaster() to also be called.
   */
  public void stopBeingMaster();
}
