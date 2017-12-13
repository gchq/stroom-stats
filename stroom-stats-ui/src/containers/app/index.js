/*
 * Copyright 2017 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import React, { Component } from 'react'
import PropTypes, { object } from 'prop-types'
import {Route, withRouter, Switch, BrowserRouter} from 'react-router-dom'
import { bindActionCreators } from 'redux'
import { connect } from 'react-redux'

import { AuthenticationRequest } from 'stroom-auth-js'
import { HandleAuthenticationResponse } from 'stroom-auth-js'

import './App.css'
//import logo from './logo.svg'
import PathNotFound from '../pathNotFound'
import Unauthorised from '../unauthorised'
import ConfigEdit from '../configEdit'

class App extends Component {
  isLoggedIn () {
    return !!this.props.idToken
  }

  render () {
    return (
      <div className='App'>
        <main className='main'>
          <div >
            <BrowserRouter basename={process.env.REACT_APP_ROOT_PATH} />
            <Switch>
              {/* Authentication routes */}
              <Route exact path={'/handleAuthentication'} component={HandleAuthenticationResponse} />
              <Route exact path={'/handleAuthenticationResponse'} component={HandleAuthenticationResponse} />

              {/* Routes not requiring authentication */}
              <Route exact path={'/unauthorised'} component={Unauthorised} />

              {/* Routes requiring authentication */}
              <Route exact path={'/config/:configId'} render={(route) => (
                this.isLoggedIn() ? <ConfigEdit /> : <AuthenticationRequest referrer={route.location.pathname} />
              )} />

              {/* Fall through to 404 */}
              <Route component={PathNotFound} />

            </Switch>
          </div>
        </main>
      </div>
    )
  }
}

App.contextTypes = {
  store: PropTypes.object,
  router: PropTypes.shape({
    history: object.isRequired
  })
}

App.propTypes = {
  idToken: PropTypes.string.isRequired
}

const mapStateToProps = state => ({
  idToken: state.authentication.idToken
})

const mapDispatchToProps = dispatch => bindActionCreators({
}, dispatch)

export default withRouter(connect(
  mapStateToProps,
  mapDispatchToProps
)(App))
