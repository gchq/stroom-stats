import { initialize } from 'redux-form'

import { configServiceUrl } from '../environmentVariables'
import { HttpError } from '../ErrorTypes'
import jwtDecode from 'jwt-decode'
import { push } from 'react-router-redux'

export const CHANGE_CONFIG = 'config/CHANGE_CONFIG'

const initialState = {
  showAlert: false,
  alertText: '',
  matchingAutoCompleteResults: [],
  errorMessage: ''
}

export default (state = initialState, action) => {
  switch (action.type) {
    case CHANGE_CONFIG:
      return {
        ...state,
        show: action.show
      }

    default:
      return state
  }
}

//export function changeVisibleContainer (container) {
//  return {
//    type: CHANGE_VISIBLE_CONTAINER,
//    show: container
//  }
//}

export const fetchConfig = (configId) => {
  return (dispatch, getState) => {

    const jwsToken = getState().authentication.idToken

    fetch(`${configServiceUrl()}/${configId}`, {
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + jwsToken
      },
      method: 'get',
      mode: 'cors'
    })
    .then(handleStatus)
    .then(response => response.json())
    .then(config => {
      dispatch({
        type: CHANGE_CONFIG,
        config
      })
      // Use the redux-form action creator to re-initialize the form with this API key
      dispatch(initialize('ConfigEditForm', config))
    })
    //.catch(error => handleErrors(error, dispatch, jwsToken))
  }
}

export const saveConfig = (editedConfig) => {
    return (dispatch, getState) => {
        
        const jwsToken = getState().authentication.idToken
        
        // TODO fill this out
        const { id } = editedConfig

    fetch(`${configServiceUrl()}/${id}`, {
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + jwsToken
      },
      method: 'put',
      mode: 'cors',
      body: JSON.stringify({
        //TODO
      })
    })
    .then(handleStatus)
    .then(() => {
        dispatch(push(`/config/${id}`))
        // TODO create snackbar to handle this message
//      dispatch(toggleAlertVisibility('Config has been updated'))
    })
//    .catch(error => handleErrors(error, dispatch, jwsToken))
  }
}


function handleStatus (response) {
  if (response.status === 200) {
    return Promise.resolve(response)
  } else if (response.status === 409) {
    return Promise.reject(new HttpError(response.status, 'This token already exists!'))
  } else {
    return Promise.reject(new HttpError(response.status, response.statusText))
  }
}

//function handleErrors (error, dispatch, token) {
//  if (error.status === 401) {
//    const decodedToken = jwtDecode(token)
//    const now = new Date().getTime() / 1000
//    const expiredToken = decodedToken.exp <= now
//    if (expiredToken) {
//      // TODO rename this to 'requestExpiredToken'
//      dispatch(requestWasUnauthorized(true))
//    } else {
//      // If it's not expired then that means this user is genuinely unauthorised
//      dispatch(relativePush('/unauthorised'))
//    }
//  } else {
//    // dispatch(errorAdd(error.status, error.message))
//  }
//}
