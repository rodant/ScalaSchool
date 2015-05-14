package suggestions
package gui

import rx.lang.scala.{Observable, Subscription}

import scala.language.reflectiveCalls
import scala.swing.Reactions.Reaction
import scala.swing.event.Event

/** Basic facilities for dealing with Swing-like components.
*
* Instead of committing to a particular widget implementation
* functionality has been factored out here to deal only with
* abstract types like `ValueChanged` or `TextField`.
* Extractors for abstract events like `ValueChanged` have also
* been factored out into corresponding abstract `val`s.
*/
trait SwingApi {

  type ValueChanged <: Event

  val ValueChanged: {
    def unapply(x: Event): Option[TextField]
  }

  type ButtonClicked <: Event

  val ButtonClicked: {
    def unapply(x: Event): Option[Button]
  }

  type TextField <: {
    def text: String
    def subscribe(r: Reaction): Unit
    def unsubscribe(r: Reaction): Unit
  }

  type Button <: {
    def subscribe(r: Reaction): Unit
    def unsubscribe(r: Reaction): Unit
  }

  implicit class TextFieldOps(field: TextField) {

    /** Returns a stream of text field values entered in the given text field.
      *
      * @param field the text field
      * @return an observable with a stream of text field updates
      */
    def textValues: Observable[String] = Observable { obs =>
      val reaction: Reaction = {
        case ValueChanged(f) => obs.onNext(f.text)
      }
      field.subscribe(reaction)
      Subscription {
        field.unsubscribe(reaction)
      }
    }

  }

  implicit class ButtonOps(button: Button) {

    /** Returns a stream of button clicks.
     *
     * @param field the button
     * @return an observable with a stream of buttons that have been clicked
     */
    def clicks: Observable[Button] = Observable { obs =>
      val reaction: Reaction = {
        case ButtonClicked(b) => obs.onNext(b)
      }
      button.subscribe(reaction)
      Subscription {
        button.unsubscribe(reaction)
      }
    }
  }

}
